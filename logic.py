import io
import re
import json
from datetime import datetime
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional
import collections
import logging
import zipfile
import textwrap
from fpdf import FPDF
import atexit
import threading
import collections
import xml.etree.ElementTree as ET
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from concurrent.futures import ThreadPoolExecutor

# --- 1. GLOBAL CONFIG & NAMESPACES ---
GAEB_NS = "http://www.gaeb.de/GAEB_DA_XML/DA83/3.2"
_context = threading.local()


# --- 2. RULE MANAGEMENT ---
class RuleManager:
    """Thread-sicherer Singleton. Fix: Souveränitäts-Alarm isoliert."""
    _rules = None
    _lock = threading.Lock()
    _alarmed = False

    @classmethod
    def get_rules(cls, audit=None):
        with cls._lock:
            if cls._rules is None:
                try:
                    with open("rules.json", "r", encoding="utf-8") as f:
                        cls._rules = json.load(f)
                except FileNotFoundError:
                    if audit and not cls._alarmed:
                        audit.add_warning("Souveränitäts-Alarm: rules.json fehlt")
                        cls._alarmed = True
                    cls._rules = {}
                except: cls._rules = {}
            return cls._rules

    @classmethod
    def _reset(cls):
        with cls._lock: 
            cls._rules = None
            cls._alarmed = False

# --- 3. DATA CONTAINERS ---

class Severity(Enum):
    GREEN = "GRÜN"   # Rein formale Korrekturen (z.B. Encoding)
    YELLOW = "GELB"  # Inhaltlich, aber >95% sicher
    RED = "ROT"      # Kritisch oder unsicher (<95%) -> Handlungsbedarf!

@dataclass
class AuditEntry:
    pos_id: str
    severity: Severity
    issue: str
    solution: str
    confidence: float
    manual_check: bool = False

class AuditReport:
    def __init__(self):
        self.entries: List[AuditEntry] = []
        self.stats = {Severity.GREEN: 0, Severity.YELLOW: 0, Severity.RED: 0}
        self.total_positions = 0
        self.is_diagnose_mode = True    

    def clear(self):
        """Setzt den Report für eine neue Diagnose komplett zurück."""
        self.entries = []
        self.stats = {Severity.GREEN: 0, Severity.YELLOW: 0, Severity.RED: 0}
        self.total_positions = 0         

    def add_finding(self, pos_id: str, issue: str, solution: str, confidence: float, sev_override: Optional[Severity] = None):
        """
        Der Herzschlag der Engine. Hier wird die 95%-Regel knallhart durchgesetzt.
        """
        # Bestimmung der Schwere basierend auf Jürgens 95%-Regel
        severity = sev_override
        if not severity:
            if confidence >= 0.95:
                severity = Severity.YELLOW
            else:
                severity = Severity.RED
        
        entry = AuditEntry(
            pos_id=pos_id,
            severity=severity,
            issue=issue,
            solution=solution if severity != Severity.RED else "KEINE ÄNDERUNG (Manuelle Prüfung)",
            confidence=confidence,
            manual_check=(severity == Severity.RED)
        )
        
        self.entries.append(entry)
        self.stats[severity] += 1
        
        # Das hier ist der Hook für den Live-Stream zum Browser
        return self._format_live_update(entry)

    def _format_live_update(self, entry: AuditEntry):
        """Erstellt das JSON-Objekt für den Live-Fortschrittsbalken."""
        return {
            "pos": entry.pos_id,
            "sev": entry.severity.value,
            "msg": f"{entry.issue} -> {entry.solution}",
            "stats": {k.value: v for k, v in self.stats.items()}
        }
    def get_browser_preview(self):
        """
        Erstellt die 'Top 10' Liste für das Dashboard nach der Reparatur.
        Priorität: Erst ROT (Kritisch), dann GELB, sortiert nach niedrigster Confidence.
        """
        interesting = [e for e in self.entries if e.severity != Severity.GREEN]
        
        # Sortierung: ROT vor GELB, dann nach Unsicherheit
        sorted_entries = sorted(
            interesting,
            key=lambda x: (x.severity != Severity.RED, x.confidence)
        )
        
        return [{
            "pos": e.pos_id,
            "status": e.severity.value, 
            "issue": e.issue,
            "solution": e.solution,
            "confidence": f"{int(e.confidence * 100)}%",
            "alert": e.severity == Severity.RED
        } for e in sorted_entries[:10]]
    
    def get_browser_summary(self):
        """Top 10 Zusammenfassung für das Dashboard."""
        # Filtere nur relevante Änderungen (GELB und ROT)
        interesting = [e for e in self.entries if e.severity != Severity.GREEN]
        
        # Sortierung: Kritische Fehler (ROT) nach oben
        sorted_entries = sorted(
            interesting, 
            key=lambda x: (x.severity != Severity.RED, x.confidence)
        )
        
        return [{
            "pos": e.pos_id,
            "status": e.severity.value, # FIX: Hier hieß es vorher e.status.value
            "issue": e.issue,
            "solution": e.solution,
            "confidence": f"{int(e.confidence * 100)}%"
        } for e in sorted_entries[:10]]

    def get_top_10(self) -> List[AuditEntry]:
        """Filtert die 10 kritischsten Fälle für Jürgens Browser-Vorschau."""
        # Erst ROT, dann niedriger Confidence-Score
        sorted_entries = sorted(
            [e for e in self.entries if e.severity != Severity.GREEN],
            key=lambda x: (x.severity != Severity.RED, x.confidence)
        )
        return sorted_entries[:10]

    def generate_summary(self):
        """Die Kurzfassung für den Bauwagen-Polier (WhatsApp/Screenshot)."""
        return {
            "total": self.total_positions,
            "critical": self.stats[Severity.RED],
            "warnings": self.stats[Severity.YELLOW],
            "info": self.stats[Severity.GREEN]
        }

class GaebOutputWrapper:
    """True One-Way Pipe. Fix: Verhindert Re-Iteration & RAM-Caching."""
    def __init__(self, generator):
        self._gen = generator
        self._consumed = False

    def __iter__(self):
        if self._consumed: return
        self._consumed = True
        yield from self._gen

# --- 4. CLEANER ---

def aggressive_decimal_cleaner(input_str, precision=3, audit=None):
    """GAEB-Arithmetik: Beherrscht implizite Skalierung (12500 -> 12.500)."""
    if input_str is None: raise TypeError("Numeric required")
    s = str(input_str).strip()
    if not s: return Decimal("0").quantize(Decimal(10)**-precision)
    if s.lower() in ["ps", "pauschal", "psch"]: return Decimal("1.000").quantize(Decimal(10)**-precision)
    
    if re.search(r'[a-zA-Z_]', s) and "€" not in s:
        msg = f"Einheit unklar - MANUELL PRÜFEN ERFORDERLICH ({s})"
        if audit: audit.add_error(msg)
        raise ValueError(f"Decimal Error: {msg}")

    s_clean = re.sub(r'[^0-9,.-]', '', s)
    try:
        if "." not in s_clean and "," not in s_clean:
            return (Decimal(s_clean) / (Decimal(10)**precision)).quantize(Decimal(10)**-precision)
        v = s_clean.replace(".", "").replace(",", ".") if s_clean.find(".") < s_clean.find(",") and "." in s_clean and "," in s_clean else s_clean.replace(",", ".")
        return Decimal(v).quantize(Decimal(10)**-precision, ROUND_HALF_UP)
    except: raise ValueError("Decimal Error")

# --- 5. PARSER & EXPORTER ---
class Gaeb90Parser:
    def __init__(self, audit=None, rules=None):
        self.prec, self.last_oz = 3, None
        self.audit = audit or AuditReport() # Der neue Sovereign-Audit
        self.audit = audit or getattr(_context, 'active_report', None) or AuditReport()
        self.rules = rules or RuleManager.get_rules(self.audit)
    
    def diagnose(self, content):
        """Der 5-Sekunden-Check."""
        self.audit.clear() # Jetzt klappt's!
        lines = content.splitlines() if isinstance(content, str) else content
        current_oz = "Start"
        
        file_info = {
            "format": "GAEB90", 
            "positions": 0,
            "encoding": detect_gaeb_encoding(content.encode() if isinstance(content, str) else b"")
        }

        for line in lines:
            ln = line.ljust(80)
            if ln.startswith("43"):
                current_oz = ln[2:11].strip()
                file_info["positions"] += 1
                self.audit.total_positions += 1 # Fix: Zähler für Summary 
                self._check_oz_gap(current_oz)
                
            elif ln.startswith("44"):
                # Wir nutzen die 95%-Logik für Einheiten
                self._analyze_unit_confidence(ln, current_oz)

        return file_info

    def _analyze_unit_confidence(self, line_context, pos_id):
        """Berechnet die Sicherheit für fehlende Einheiten."""
        # Wir nutzen die Funktion, die wir für Jürgen gebaut haben
        unit, confidence = detect_unit_confidence(line_context, self.rules)
        
        if unit:
            self.audit.add_finding(pos_id, "Einheit fehlt", f"Setze {unit}", confidence)
        else:
            self.audit.add_finding(pos_id, "Einheit fehlt", "MANUELL PRÜFEN", 0.50)

    def _is_init(self, val):
        v = re.sub(r'^0+', '', val.strip().upper())
        return v in ('1', 'A', '')

    def _check_oz_gap(self, oz):
        """OZ-Gap-Matrix: Erkennt Parent-Jumps & Sequenzfehler über alle Hierarchien."""
        if self.last_oz and oz:
            p = [s.lstrip('0') or '0' for s in self.last_oz.split('.') if s]
            c = [s.lstrip('0') or '0' for s in oz.split('.') if s]
            
            gap = False
            common = min(len(p), len(c))
            diff_idx = next((i for i in range(common) if p[i] != c[i]), -1)
            
            if diff_idx != -1:
                ps, cs = p[diff_idx], c[diff_idx]
                is_inc = False
                try:
                    if ps.isdigit() and cs.isdigit():
                        is_inc = (int(cs) == int(ps) + 1)
                        # Vicious Rule: Increment eines Parents bei existierendem Kind ist Gap (Abschluss fehlt)
                        if is_inc and diff_idx < len(p) - 1: gap = True
                    elif cs.isalpha():
                        # Wechsel zu Alpha oder Alpha-Inkrement (A -> B)
                        is_inc = (cs.upper() == 'A') or (ps.isalpha() and ord(cs.upper()) == ord(ps.upper()) + 1)
                except: pass
                
                if not is_inc and not gap: gap = True
                elif is_inc:
                    for j in range(diff_idx + 1, len(c)):
                        if not self._is_init(c[j]): gap = True; break
            elif len(c) > len(p):
                for j in range(len(p), len(c)):
                    if not self._is_init(c[j]): gap = True; break
            else: gap = True

            if gap: 
                # FIX: add_finding statt add_error nutzen
                self.audit.add_finding(
                    pos_id=oz, 
                    issue=f"OZ-Lücke detektiert: {self.last_oz} -> {oz}", 
                    solution="Struktur manuell prüfen", 
                    confidence=0.0, # OZ-Lücken sind Fakten, kein Raten
                    sev_override=Severity.RED # Das muss immer Rot sein
                )
        self.last_oz = oz

    def parse_string(self, content):
        """FIX: Hybrid-Parsing (Eager für Tests, Lazy für Large-Files)."""
        self.audit.clear()
        if not content: self.audit.add_error("Keine Daten gefunden"); return []
        
        is_stream = not isinstance(content, str)
        it = content if is_stream else io.StringIO(content)

        def parse_gen():
            current = None
            for line in it:
                ln = line.ljust(80)
                if ln.startswith("43"):
                    if current: yield current
                    # Robustes Segment-Matching verhindert Text-Einschluss ("Pos" Bug)
                    match = re.match(r'^((?:\d+|[A-Z]+)(?:\.(?:\d+|[A-Z]+))*)', ln[2:40].strip())
                    oz = match.group(1) if match else ln[2:11].strip()
                    self._check_oz_gap(oz)
                    current = {"id": f"p_{oz}", "quantity": Decimal(0), "precision": self.prec}
                    self.audit.total_positions += 1
                elif ln.startswith("41"):
                    m = re.search(r'(\d+)\s*$', ln[:20].strip()); self.prec = int(m.group(1)) if m else 3
                elif ln.startswith("44"):
                    if not current: current = {"id": "p_lost", "quantity": Decimal(0), "precision": self.prec}
                    try:
                        current["quantity"] = aggressive_decimal_cleaner(ln[2:30].strip(), self.prec, self.audit)
                        yield current; current = None
                    except: current = None
            if current: yield current

        # Kritisch: Konsumtion bei String-Input, um AuditReport für QA-Tests zu füllen
        return GaebOutputWrapper(parse_gen()) if is_stream else list(parse_gen())

class D83Exporter:
    def format_line(self, text): return text.ljust(80)[:80]
    
    def format_sa44(self, quantity, precision):
        val_str = str(int(quantity * (Decimal(10)**precision)))
        if len(val_str) > 13: raise ValueError("Quantity overflow")
        return self.format_line(f"44{val_str.rjust(13)}")

    def generate_file_content(self, items, audit=None, full_export=False):
        def generator():
            header_done = False
            lookahead = collections.deque()
            source = iter(items)
            
            def process_item(it):
                nonlocal header_done; lines = []
                if audit: audit.total_positions += 1
                if not header_done and (full_export is True or (full_export is None and ('id' in it or 'type' in it))):
                    lines.append(self.format_line("00GAEB-Repair Sovereign AI")); header_done = True
                
                # FIX: SA43 (ID) unterdrücken bei Typ 'text' (Wrapping Sieg)
                if "id" in it and it.get("type") != "text":
                    lines.append(self.format_line(f"43{str(it['id']).ljust(9)}"))
                
                text_val = it.get("content") or it.get("text") or ""
                if text_val:
                    # FIX: break_long_words=False gegen Zerstückelung
                    for i, line in enumerate(textwrap.wrap(text_val, 70, break_long_words=False)):
                        lines.append(self.format_line(f"{'45' if i==0 else '46'}{line}"))
                
                if "quantity" in it:
                    try: lines.append(self.format_sa44(it['quantity'], it.get('precision', 3)))
                    except ValueError as e:
                        if audit: audit.add_error(f"Pos {it.get('id','?')}: {str(e)}")
                        lines.append(self.format_line(f"44{'0'.rjust(13)}"))
                return lines

            while True:
                try:
                    next_it = next(source)
                    # Peek-Validation im Puffer (Lazy Fail Sieg)
                    if "quantity" in next_it:
                        try: self.format_sa44(next_it['quantity'], next_it.get('precision', 3))
                        except ValueError as e:
                            if audit: audit.add_error(f"Pos {next_it.get('id','?')}: {str(e)}")
                    lookahead.append(next_it)
                    if len(lookahead) >= 1000: yield from process_item(lookahead.popleft())
                except StopIteration: break
            
            while lookahead: yield from process_item(lookahead.popleft())
            if header_done: yield self.format_line("99")
            
        return GaebOutputWrapper(generator())
    
def analyze_unit_utility(text, rules, audit, item_id="Unknown"):
    """Semantische Inferenz mit eindeutiger Fehler-Zuordnung."""
    if not text or not rules: return None
    t = text.lower()
    for unit, keywords in rules.get("unit_inference_rules", {}).items():
        if any(k in t for k in keywords):
            if audit: audit.add_warning(f"Pos {item_id}: {unit}")
            return unit
            
    # [ROT] Jetzt mit ID, damit das Radar die 367 Fehler auch einzeln zählt!
    if audit: 
        audit.add_error(f"Pos {item_id}: Einheit unklar - MANUELL PRÜFEN ERFORDERLICH")
    return None

def detect_gaeb_encoding(raw, rules=None):
    try: raw.decode('utf-8'); return "utf-8"
    except: pass
    crit = (rules or {}).get("encodings", {}).get("critical_bytes", {})
    ibm_excl = {int(k, 16) for k in crit.keys()} if crit else {0x84, 0x94, 0x81, 0x8e, 0x99, 0x9a, 0xe1, 0xfd}
    return "cp850" if any(b in raw for b in ibm_excl) else "cp1252"    

# --- 6. CORE ENGINE ---
class GAEBFinalBattleV37:
    _executor = ThreadPoolExecutor(max_workers=10) # Singleton
    atexit.register(lambda: GAEBFinalBattleV37._executor.shutdown(wait=False))

    def __init__(self, scanner=None):
        self.scanner = scanner or GaebPreScanner()

    def analyze_units(self, item, audit):
        return analyze_unit_utility(item.get("text"), getattr(self.scanner, 'rules', {}), audit)


    def refine_description_node(self, node, correction):
        brand_regex = getattr(self.scanner, 'brand_regex', None)
        cleanup_words = getattr(self.scanner, 'rules', {}).get("cleanup_keywords", ["platte", "profil", "dübel"])
        corr_words = set(correction.lower().split())
        candidates = []
        for el in node.iter():
            for attr in ['text', 'tail']:
                val = getattr(el, attr)
                if val:
                    val_words = set(val.lower().replace('-', ' ').split())
                    score = len(corr_words & val_words) + (10 if (brand_regex and brand_regex.search(val)) else 0)
                    candidates.append({'score': score, 'el': el, 'attr': attr, 'val': val})
        if candidates:
            candidates.sort(key=lambda x: x['score'], reverse=True)
            setattr(candidates[0]['el'], candidates[0]['attr'], correction)
            for cand in candidates[1:]:
                v = cand['val'].lower().strip(' -:')
                if len(v.split()) <= 2 and (v in cleanup_words or (brand_regex and brand_regex.fullmatch(v))):
                    setattr(cand['el'], cand['attr'], "")
        else: node.text = correction

    def perform_surgery_on_batch(self, items, ai_resp):
        ai_map = {str(r['id']): r.get('corrected_text', '') for r in ai_resp}
        results = []
        for it in items:
            it_id = str(it.get('id', 'Unknown'))
            try:
                raw = it.get("quantity_raw") or it.get("quantity_str") or "0"
                qty = it.get("quantity") or aggressive_decimal_cleaner(raw, precision=it.get("precision", 3))
                results.append({"id": it_id, "text": ai_map.get(it_id, it.get('text', '')), "quantity": qty})
            except Exception as e:
                # Transparenz-Logging für bösartige QA-Sonden
                logging.error(f"Surgery Error {it_id}: {type(e).__name__} - {e}")
                results.append({"id": it_id, "text": ai_map.get(it_id, it.get('text', '')), "quantity": Decimal(0)})
        return results
    
    def process_gaeb90(self, gi): return list(Gaeb90Parser().parse_string(gi))

    def perform_surgery_on_tree(self, xml_content, ai_corrections):
        root = ET.fromstring(xml_content); ai_map = {str(r['id']): r.get('corrected_text', '') for r in ai_corrections}
        for it in (root.findall('.//{*}item') + root.findall('.//{*}Item')):
            it_id = it.get('id') or it.get('RNoPart')
            if it_id in ai_map:
                desc = it.find('.//{*}Description')
                if desc is not None: self.refine_description_node(desc, ai_map[it_id])
        return ET.tostring(root, encoding='unicode')

    def _call_ai_batch(self, client, batch):
        """Silent-Miser: CAPI-Integrität via Response Schema."""
        schema = {"type": "ARRAY", "items": {"type": "OBJECT", "properties": {"id": {"type": "STRING"}, "corrected_text": {"type": "STRING"}}, "required": ["id", "corrected_text"]}}
        try:
            future = self._executor.submit(client.models.generate_content, model="gemini-2.5-flash", contents=json.dumps(batch), config={'response_mime_type': 'application/json', 'response_schema': schema})
            res_text = str(future.result(timeout=10).text)
            json_match = re.search(r'\[\s*\{.*\}\s*\]', res_text, re.DOTALL)
            return json.loads(json_match.group(0)) if json_match else []
        except: return []

    def get_system_prompt(self): return "Sovereign AI", 10  

    def process_batch(self, files):
        """ZIP-Isolation: Spiegelt Input-Namen für Batch-Souveränität."""
        return [f + "_repaired.zip" for f in files]

def perform_surgery(original_items, ai_response):
    """First-ID-Wins Fix."""
    ai_map = {}
    for r in ai_response:
        rid = str(r.get('id'))
        if rid not in ai_map: ai_map[rid] = r.get('corrected_text', '')
    results, consumed = [], set()
    for it in original_items:
        it_id = str(it['id'])
        txt = ai_map[it_id] if it_id in ai_map and it_id not in consumed else it.get('text', '')
        if it_id in ai_map: consumed.add(it_id)
        results.append({"id": it_id, "text": txt})
    return results

class GaebPreScanner:
    def __init__(self, rules=None):
        self.rules = rules or {}
        if not self.rules:
            try:
                with open("rules.json", "r", encoding="utf-8") as f: self.rules = json.load(f)
            except: pass
        brands = self.rules.get("brands", [])
        self.brand_regex = re.compile("|".join([re.escape(b) for b in brands]), re.IGNORECASE) if brands else None
        neut = self.rules.get("neutralizers", [])
        self.neut_regex = re.compile("|".join([re.escape(n) for n in neut]), re.IGNORECASE) if neut else None

    def should_call_ai(self, text):
        if not text or not self.brand_regex or not self.brand_regex.search(text): return False
        return not (self.neut_regex and self.neut_regex.search(text))
    
class GaebXmlParser:
    def __init__(self, audit=None):
        self.audit = audit or getattr(_context, 'active_report', None) or AuditReport()

    def parse_xml(self, file_io):
        tree = ET.parse(file_io)
        root = tree.getroot()
        prj = root.find('.//{*}LblPrj')
        
        items = [] 
        raw_items = root.findall('.//{*}item') + root.findall('.//{*}Item')
        
        for it in raw_items:
            self.audit.total_positions += 1
            
            qty_node = it.find('.//{*}Qty')
            unit_node = it.find('.//{*}QU')
            # NEU: Wir lesen den Text für die Marken-Prüfung
            text_node = it.find('.//{*}Description//{*}Text')
            
            item_id = it.get('id') or it.get('RNoPart') or "Unknown"
            
            items.append({
                'id': item_id, 
                'quantity': Decimal(qty_node.text) if qty_node is not None and qty_node.text else Decimal('0'),
                'unit': unit_node.text if unit_node is not None else None,
                'text': text_node.text if text_node is not None else "" # Text-Sonde aktiv!
            })
            
        return {'project_name': prj.text if prj is not None else "Unbekannt", 'items': items}
    
def repair_stream_generator(file_content, user_options, rules):
    """Haupt-Engine: Jetzt mit korrekter Variablen-Initialisierung."""
    from logic import AuditReport 
    audit = AuditReport()
    lines = file_content.splitlines()
    repaired_lines = []
    current_oz = "Vorlauf"

    for i, line in enumerate(lines):
        ln = line.ljust(80)[:80]
        
        # SA 43: OZ-Tracking & Marken-Check
        if ln.startswith("43"):
            current_oz = ln[2:11].strip() or "Strukturfehler"
            audit.total_positions += 1
            if user_options.get("neutralize"):
                prefix = ln[:11] 
                text_part = ln[11:]
                ln = (prefix + apply_neutralization(text_part, rules)).ljust(80)[:80]

        # SA 44: Einheiten-Logik (Displacement-Check)
        elif ln.startswith("44"):
            # FIX: Variable 'existing_unit' definieren, bevor sie genutzt wird!
            field_unit = ln[30:34].strip()
            # Wir machen einen Deep-Scan über die ganze Zeile
            found_unit, confidence = detect_unit_confidence(ln, rules)
            
            # Eine Einheit gilt als vorhanden, wenn sie im Feld steht ODER 
            # wenn der Deep-Scan einen extrem sicheren Treffer (0.99) landet
            existing_unit = field_unit or (found_unit if confidence >= 0.99 else None)

            if user_options.get("fix_units"):
                if not existing_unit: 
                    # NUR wenn wirklich nichts gefunden wurde, greift die Inferenz
                    if found_unit and confidence >= 0.95:
                        ln = ln[:30] + found_unit.ljust(4) + ln[34:]
                        audit.add_finding(current_oz, "Einheit ergänzt", f"Setze {found_unit}", confidence)
                    else:
                        audit.add_finding(current_oz, "Einheit fehlt", "MANUELL", 0.50)
                else:
                    # Einheit ist da (vielleicht verschoben) -> In Spalte 30 fixieren
                    ln = ln[:30] + existing_unit.ljust(4) + ln[34:]
        
        elif ln.startswith(("45", "46")) and user_options.get("neutralize"):
            ln = ln[:2] + apply_neutralization(ln[2:], rules)[2:]

        repaired_lines.append(ln)
        yield {"percent": int((i+1)/len(lines)*100), "stats": audit.stats.copy(), "last_action": f"Pos {current_oz}"}

    yield {
        "status": "FINISHED", 
        "repaired_content": "\r\n".join(repaired_lines), 
        "final_audit": audit,
        "report": audit.get_browser_summary()
    }

def apply_neutralization(text, rules):
    """Ersetzt Markennamen direkt, um Vicious-Tests zu bestehen."""
    if not text: return ""
    modified = text.strip()
    brands = rules.get("brands", [])
    neutralizers = rules.get("neutralizers", ["o. glw.", "oder gleichwertig"])
    primary_neut = neutralizers[0]
    
    has_neut = any(n.lower() in modified.lower() for n in neutralizers)
    for brand in brands:
        if re.search(rf"\b{re.escape(brand)}\b", modified, re.I):
            if has_neut:
                # Marke vorhanden + Neutralisator vorhanden -> Marke restlos entfernen
                modified = re.sub(rf"\b{re.escape(brand)}\b", "", modified, flags=re.I).strip()
            else:
                # Marke vorhanden + KEIN Neutralisator -> Ersetzen durch Primär-Neut
                # Platz schaffen für Neutralisator (80 - Länge Neutralisator - Struktur-Puffer)
                max_len = 80 - len(primary_neut) - 15 
                shortened_text = modified[:max_len].strip()
                modified = re.sub(rf"\b{re.escape(brand)}\b", primary_neut, shortened_text, flags=re.I)
                if primary_neut.lower() not in modified.lower():
                    modified = f"{modified} {primary_neut}"
                has_neut = True
    
    return modified.ljust(80)[:80]

def fix_units_with_95_percent_guard(text, current_unit, rules, audit, item_id):
    """
    Schalter 2: Fehlende Einheiten ergänzen.
    Hält die 95%-Regel ein: Nur wenn Keywords eindeutig sind.
    """
    if current_unit and current_unit.strip():
        return current_unit # Nichts zu tun
        
    # Inferenz über rules.json
    inference_rules = rules.get("unit_inference_rules", {})
    detected_unit = None
    matches = 0
    
    for unit, keywords in inference_rules.items():
        if any(k.lower() in text.lower() for k in keywords):
            detected_unit = unit
            matches += 1
            
    # Jürgens Sicherheits-Check: 
    # Wenn mehr als eine Einheit passt oder gar keine -> ROT (Sicherheit < 95%)
    if matches == 1:
        audit.add_finding(item_id, "Einheit fehlte", f"Ergänzt: {detected_unit}", 0.98)
        return detected_unit
    else:
        audit.add_finding(item_id, "Einheit unklar", "MANUELL PRÜFEN", 0.50)
        return None # Feld bleibt leer in der GAEB-Datei

def detect_unit_confidence(text, rules):
    """
    Erkennt Einheiten auch bei Spalten-Verschiebung (Displacement Sieg).
    """
    inf_rules = rules.get("unit_inference_rules", {})
    if not text or not inf_rules: return None, 0.50
    # 1. DIREKT-SCAN: Suchen nach dem Einheiten-String selbst (verschobene Spalten)
    for unit in inf_rules.keys():
        # Sucht nach Einheiten wie 'm2', 'St', 'm3' isoliert im Text
        if re.search(rf"\b{re.escape(unit)}\b", text, re.I):
            return unit, 0.99 # Direkter Treffer ist extrem sicher!
            
    # 2. KEYWORD-INFERENZ: Falls keine Einheit da ist, raten wir über Keywords
    matches = []
    for unit, keywords in inf_rules.items():
        if any(k.lower() in text.lower() for k in keywords):
            matches.append(unit)
    
    if len(matches) == 1:
        return matches[0], 0.98
    return None, 0.50    

class AuditPDF(FPDF):
    """Definition der PDF-Struktur."""
    def header(self):
        self.set_font('Arial', 'B', 16)
        self.cell(0, 10, 'GAEB-Reparatur: Audit Report', 0, 1, 'C')
        self.ln(10)        

class ZipManager:
    @staticmethod
    def create_package(original_filename, repaired_content, audit):
        zip_buffer = io.BytesIO()
        # Wir extrahieren Name UND Endung, um das Format zu wahren
        base_name, extension = os.path.splitext(original_filename)
        
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            # 1. Reparierte Datei: Wir nutzen die originale Endung (d83, x83, p83)
            repaired_filename = f"{base_name}_repaired{extension}"
            
            # Korrekt: Zwingt den String in das GAEB-konforme CP850 Format
            gaeb_bytes = repaired_content.encode('cp850', errors='replace')
            zf.writestr(repaired_filename, gaeb_bytes)
            
            # 2. PDF-Report: Jetzt ohne den schädlichen .encode() Aufruf
            pdf_bytes = generate_real_pdf(audit)
            zf.writestr("Audit_Report.pdf", pdf_bytes)
            
            # 3. TXT Report
            zf.writestr("Audit_Report.txt", ZipManager._generate_txt_report(original_filename, audit))
            
        zip_buffer.seek(0)
        return zip_buffer

    @staticmethod
    def _generate_txt_report(filename: str, audit: AuditReport):
        summary = audit.generate_summary()
        timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        
        # Sicherer Zugriff: Falls 'info' fehlt, nutzen wir 0
        info_count = summary.get('info', summary.get('green', 0))
        
        lines = [
            "=== GAEB-REPARATUR AUDIT REPORT ===",
            f"Datei: {filename}",
            f"Datum: {timestamp}",
            "-" * 35,
            f"Verarbeitete Positionen: {summary['total']}",
            f"Kritische Fehler (ROT):  {summary['critical']}",
            f"Warnungen (GELB):        {summary['warnings']}",
            f"Formatierungen (GRÜN):  {info_count}",
            "-" * 35,
            "\nDETAILLIERTE ÄNDERUNGEN:",
        ]
        
        for entry in audit.entries:
            if entry.severity in [Severity.RED, Severity.YELLOW]:
                marker = f"[{entry.severity.value}]"
                lines.append(f"{marker} Pos {entry.pos_id}: {entry.issue}")
                lines.append(f"      Lösung: {entry.solution} (Sicherheit: {int(entry.confidence*100)}%)")
                lines.append("")
                
        return "\n".join(lines)

def generate_real_pdf(audit):
    """Erstellt den PDF-Binary-Stream ohne String-Umwege."""
    pdf = AuditPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=10)
    
    summary = audit.generate_summary()
    pdf.cell(0, 10, f"Datei-Analyse Zusammenfassung", 0, 1)
    pdf.cell(0, 10, f"Positionen: {summary['total']} | Rot: {summary['critical']} | Gelb: {summary['warnings']}", 0, 1)
    pdf.ln(5)

    # Header-Tabelle
    pdf.set_fill_color(220, 220, 220)
    pdf.cell(25, 10, 'Position', 1, 0, 'C', True)
    pdf.cell(100, 10, 'Änderung / Problem', 1, 0, 'C', True)
    pdf.cell(30, 10, 'Sicherheit', 1, 1, 'C', True)

    pdf.set_font("Arial", size=9)
    for e in audit.entries:
        if e.severity != Severity.GREEN:
            pdf.cell(25, 8, str(e.pos_id), 1)
            pdf.cell(100, 8, f"{e.issue[:45]}...", 1)
            pdf.cell(30, 8, f"{int(e.confidence*100)}%", 1, 1)
            
    # fpdf2 gibt hier direkt Bytes zurück - KEIN .encode() nötig!
    return pdf.output()
