import streamlit as st
import time
import io
from logic import Gaeb90Parser, RuleManager, repair_stream_generator, ZipManager, Severity

# Seite konfigurieren
st.set_page_config(page_title="GAEB Sovereign Repair", page_icon="🏗️")

st.title("🏗️ GAEB Sovereign Repair Engine")
st.markdown("---")

# 1. DER WORKFLOW: Sekunde 0-5 (Drag & Drop)
uploaded_file = st.file_uploader("Zieh deine kaputte GAEB-Datei hier rein", type=["d83", "x83", "p83"])

if uploaded_file:
    # Lade Regeln & Parser
    rules = RuleManager.get_rules()
    content = uploaded_file.getvalue().decode("cp850") # Default GAEB-Encoding
    parser = Gaeb90Parser(rules=rules)

    # 2. DIAGNOSE: Sekunde 5-10 (Schnell-Check)
    with st.status("Analysiere Datei...", expanded=True) as status:
        diag_info = parser.diagnose(content)
        st.write(f"✓ Datei erkannt: {uploaded_file.name} ({diag_info['positions']} Positionen)")
        
        # Ampel-Display
        col1, col2, col3 = st.columns(3)
        col1.metric("🔴 ROT", f"{parser.audit.stats[Severity.RED]} Kritisch")
        col2.metric("🟡 GELB", f"{parser.audit.stats[Severity.YELLOW]} Warnungen")
        col3.metric("🟢 GRÜN", f"{parser.audit.stats[Severity.GREEN]} OK")
        status.update(label="Diagnose abgeschlossen!", state="complete")

    st.markdown("---")
    
    # 3. ENTSCHEIDUNG: Jürgens 3 Hauptschalter
    st.subheader("Was soll repariert werden?")
    opt_neutralize = st.checkbox("Herstellernamen automatisch neutralisieren", value=True)
    opt_units = st.checkbox("Fehlende Einheiten ergänzen (>95% Sicherheit)", value=True)
    opt_oz = st.checkbox("OZ-Konflikte automatisch korrigieren", value=True)

    user_options = {
        "neutralize": opt_neutralize,
        "fix_units": opt_units,
        "fix_oz": opt_oz
    }

    if st.button("🚀 JETZT REPARIEREN"):
        # Platzhalter für das Ergebnis der Reparatur
        repaired_file_content = None
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 4. REPARATUR-SCHLEIFE: Wir konsumieren den Generator
        for update in repair_stream_generator(content, user_options, rules):
            if "percent" in update:
                progress_bar.progress(update["percent"])
                status_text.text(update["last_action"])
            
            # Wenn der Generator fertig ist, fangen wir den Inhalt ab
            if update.get("status") == "FINISHED":
                repaired_file_content = update.get("repaired_content")
                st.success("Reparatur abgeschlossen!")
                
                # 5. RESULTAT: Die 'Top 10' Liste für Jürgen
                st.subheader("=== TOP 10 ÄNDERUNGEN (PRÜFPFLICHTIG) ===")
                st.table(update["report"])
                
                # Das ZIP-Paket wird JETZT mit den echten Daten geschnürt
                zip_buffer = ZipManager.create_package(
                    original_filename=uploaded_file.name, 
                    repaired_content=repaired_file_content, 
                    audit=parser.audit
                )
                
                # Der finale Button
                st.download_button(
                    label="📥 DOWNLOAD ZIP (Reparierte Datei + Report)",
                    data=zip_buffer,
                    file_name=f"{uploaded_file.name}_repaired_package.zip",
                    mime="application/zip"
                )
