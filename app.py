import streamlit as st
import io
from logic import Gaeb90Parser, RuleManager, repair_stream_generator, ZipManager, Severity

# --- 1. KONFIGURATION & HELFER ---
st.set_page_config(page_title="GAEB Sovereign Repair", page_icon="🏗️")

def render_ampel(stats):
    """
    Robustes Ampel-Display, das Enum-Keys und String-Keys versteht.
    """
    def get_val(sev):
        # Prüft erst das Enum-Objekt, dann den String-Wert
        return stats.get(sev, stats.get(sev.value, 0))

    col1, col2, col3 = st.columns(3)
    col1.metric("🔴 ROT", f"{get_val(Severity.RED)} Kritisch")
    col2.metric("🟡 GELB", f"{get_val(Severity.YELLOW)} Warnungen")
    col3.metric("🟢 GRÜN", f"{get_val(Severity.GREEN)} OK")

st.title("🏗️ GAEB Sovereign Repair Engine")
st.markdown("---")

# --- 2. UPLOAD & INITIALISIERUNG ---
uploaded_file = st.file_uploader("Zieh deine kaputte GAEB-Datei hier rein", type=["d83", "x83", "p83"])

if uploaded_file:
    # Ab hier sind 'rules', 'content' und 'parser' definiert
    rules = RuleManager.get_rules()
    content = uploaded_file.getvalue().decode("cp850") 
    parser = Gaeb90Parser(rules=rules)

    # --- 3. DIAGNOSE (Sekunde 5-10) ---
    with st.status("Analysiere Datei...", expanded=True) as status:
        diag_info = parser.diagnose(content)
        st.write(f"✓ Datei erkannt: {uploaded_file.name} ({diag_info['positions']} Positionen)")
        
        # Ampel-Anzeige direkt aufrufen
        ampel_placeholder = st.empty()
        with ampel_placeholder.container():
            render_ampel(parser.audit.stats)
            
        status.update(label="Diagnose abgeschlossen!", state="complete")

    st.markdown("---")
    
    # --- 4. ENTSCHEIDUNG (Jürgens 3 Schalter) ---
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
        
        # --- 5. REPARATUR-SCHLEIFE ---
        for update in repair_stream_generator(content, user_options, rules):
            if "percent" in update:
                progress_bar.progress(update["percent"])
                status_text.text(update["last_action"])
                # Live-Update der Ampel
                with ampel_placeholder.container():
                    render_ampel(update["stats"])
            
            if update.get("status") == "FINISHED":
                repaired_file_content = update.get("repaired_content")
                final_audit = update.get("final_audit")
                
                st.success("Reparatur abgeschlossen!")
                
                # Top 10 Ergebnis-Vorschau
                st.subheader("=== TOP 10 ÄNDERUNGEN (PRÜFPFLICHTIG) ===")
                st.table(update["report"])
                
                # ZIP-Paket Erstellung
                zip_buffer = ZipManager.create_package(
                    original_filename=uploaded_file.name, 
                    repaired_content=repaired_file_content, 
                    audit=final_audit
                )
                
                st.download_button(
                    label="📥 DOWNLOAD ZIP-PAKET",
                    data=zip_buffer,
                    file_name=f"{uploaded_file.name}_repariert.zip",
                    mime="application/zip"
                )
