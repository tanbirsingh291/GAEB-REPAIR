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
        # 4. REPARATUR: Sekunde 15-60 (Live-Feedback)
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_area = st.empty()

        for update in repair_stream_generator(content, user_options, rules):
            if "percent" in update:
                progress_bar.progress(update["percent"])
                status_text.text(update["last_action"])
                # Live-Counter-Update oben rechts simulieren
            
            if update.get("status") == "FINISHED":
                st.success("Reparatur abgeschlossen!")
                
                # 5. RESULTAT: Top 10 Vorschau & Download
                st.subheader("=== TOP 10 ÄNDERUNGEN ===")
                st.table(update["report"])
                
                # ZIP-Paket schnüren
                zip_data = ZipManager.create_package(uploaded_file.name, content, parser.audit)
                st.download_button(
                    label="📥 DOWNLOAD ZIP-PAKET",
                    data=zip_data,
                    file_name=f"{uploaded_file.name}_repariert.zip",
                    mime="application/zip"
                )
