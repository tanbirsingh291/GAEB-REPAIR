import streamlit as st
import io
from logic import Gaeb90Parser, RuleManager, repair_stream_generator, ZipManager, Severity

# Seite konfigurieren
st.set_page_config(page_title="GAEB Sovereign Repair", page_icon="🏗️")

st.title("🏗️ GAEB Sovereign Repair Engine")
st.markdown("---")

# 1. DER WORKFLOW: Sekunde 0-5 (Drag & Drop) 
uploaded_file = st.file_uploader("Zieh deine kaputte GAEB-Datei hier rein", type=["d83", "x83", "p83"])

if uploaded_file:
    rules = RuleManager.get_rules()
    content = uploaded_file.getvalue().decode("cp850") # GAEB90 Standard 
    parser = Gaeb90Parser(rules=rules)

    # 2. DIAGNOSE: Sekunde 5-10 
    with st.status("Analysiere Datei...", expanded=True) as status:
        diag_info = parser.diagnose(content)
        st.write(f"✓ Datei erkannt: {uploaded_file.name} ({diag_info['positions']} Positionen)")
        
        # Platzhalter für die Live-Ampel 
        ampel_placeholder = st.empty()
        
        def render_ampel(stats):
            with ampel_placeholder.container():
                c1, c2, c3 = st.columns(3)
                c1.metric("🔴 ROT", f"{stats[Severity.RED]} Kritisch")
                c2.metric("🟡 GELB", f"{stats[Severity.YELLOW]} Warnungen")
                c3.metric("🟢 GRÜN", f"{stats[Severity.GREEN]} OK")

        render_ampel(parser.audit.stats)
        status.update(label="Diagnose abgeschlossen!", state="complete")

    st.markdown("---")
    
    # 3. ENTSCHEIDUNG: Jürgens 3 Hauptschalter 
    st.subheader("Was soll repariert werden?")
    user_options = {
        "neutralize": st.checkbox("Herstellernamen automatisch neutralisieren", value=True),
        "fix_units": st.checkbox("Fehlende Einheiten ergänzen (>95% Sicherheit)", value=True),
        "fix_oz": st.checkbox("OZ-Konflikte automatisch korrigieren", value=True)
    }

    if st.button("🚀 JETZT REPARIEREN"):
        repaired_file_content = None
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 4. REPARATUR-SCHLEIFE (Live-Updates) 
        for update in repair_stream_generator(content, user_options, rules):
            if "percent" in update:
                progress_bar.progress(update["percent"])
                status_text.text(update["last_action"])
                # Live-Update der Ampel während der Reparatur 
                render_ampel(update["stats"])
            
            if update.get("status") == "FINISHED":
                repaired_file_content = update.get("repaired_content")
                # Wir nehmen den echten Audit-Report der Reparatur
                final_audit_report = update.get("final_audit") 
                
                st.success("Reparatur abgeschlossen!")
                
                st.subheader("=== TOP 10 ÄNDERUNGEN (PRÜFPFLICHTIG) ===")
                st.table(update["report"])
                
                # Jetzt hat der ZipManager alles, was er braucht
                zip_buffer = ZipManager.create_package(
                    original_filename=uploaded_file.name, 
                    repaired_content=repaired_file_content, 
                    audit=final_audit_report
                )
                
                st.download_button(
                    label="📥 DOWNLOAD ZIP-PAKET",
                    data=zip_buffer,
                    file_name=f"{uploaded_file.name}_repariert.zip",
                    mime="application/zip"
                )
