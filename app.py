import streamlit as st
import json
from logic import Gaeb90Parser, AuditReport, Severity, apply_neutralization, fix_units_with_95_percent_guard, ZipManager

st.set_page_config(page_title="Sovereign AI - GAEB Repair", layout="wide")

# 1. Setup & Konfiguration
with open("rules.json", "r", encoding="utf-8") as f:
    rules = json.load(f)

st.title("🏗️ GAEB-Reparatur Dashboard")
st.write("Neutralisierung von Marken und Heilung von Einheiten nach der 95%-Regel.")

# 2. File Upload
uploaded_file = st.file_uploader("Lade eine GAEB-Datei hoch (.d83)", type=["d83"])

if uploaded_file:
    content = uploaded_file.getvalue().decode("utf-8")
    audit = AuditReport()
    parser = Gaeb90Parser(audit=audit, rules=rules)
    
    # Verarbeitung starten
    with st.spinner('Engine analysiert die Datei...'):
        items = list(parser.parse_string(content))
        processed_items = []
        
        for it in items:
            # Simulation der Text-Analyse für den Stress-Test
            # Hier greifen wir auf die Marken aus rules.json zu (z.B. Hilti, Knauf)
            pos_id = it['id']
            # Hinweis: In der echten App würde hier der Text aus SA45/46 extrahiert
            original_text = "Beispieltext mit Hilti" if "02" in pos_id else "Beton lieferung" 
            
            # Neutralisierung anwenden
            clean_text = apply_neutralization(original_text, rules)
            
            # Einheit heilen (95%-Regel)
            fixed_unit = fix_units_with_95_percent_guard(original_text, None, rules, audit, pos_id)
            
            processed_items.append({"id": pos_id, "text": clean_text, "unit": fixed_unit})

    # 3. Anzeige der Ergebnisse (Ampel-System)
    col1, col2, col3 = st.columns(3)
    summary = audit.generate_summary()
    col1.metric("Kritisch (ROT)", summary['critical'])
    col2.metric("Warnungen (GELB)", summary['warnings'])
    col3.metric("Info (GRÜN)", summary['info'])

    # Top 10 Korrekturen im Browser-Preview
    st.subheader("Top 10 Audit-Findings")
    preview = audit.get_browser_preview()
    if preview:
        st.table(preview)
    else:
        st.success("Keine kritischen Fehler gefunden!")

    # 4. Download der reparierten Datei
    # Hier nutzen wir den ZipManager, um das Paket aus repaired.d83 und Audit.pdf zu packen
    st.subheader("Download")
    # (Platzhalter für den tatsächlichen Datei-Export-String)
    repaired_content = "GAEB-REPAIRED-CONTENT" 
    zip_buffer = ZipManager.create_package(uploaded_file.name, repaired_content, audit)
    
    st.download_button(
        label="Repariertes Paket herunterladen (ZIP)",
        data=zip_buffer,
        file_name=f"Reparatur_{uploaded_file.name}.zip",
        mime="application/zip"
    )
