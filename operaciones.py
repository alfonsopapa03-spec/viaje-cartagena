"""
Sistema de Gestión de Proveedores
Versión 2.2 - PostgreSQL puro (BD + PDFs en BYTEA)
Contexto: Colombia
"""

import streamlit as st
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import pandas as pd
import io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import plotly.express as px

# ==================== CONFIGURACIÓN SUPABASE ====================
SUPABASE_DB_URL = "postgresql://postgres.wiomyjrmsrhcgvhgkbqe:Conejito800$@aws-1-us-west-2.pooler.supabase.com:6543/postgres"

# ==================== DOCUMENTOS REQUERIDOS ====================
DOCUMENTOS = {
    'doc_rut':          '1. RUT',
    'doc_ccio':         '2. C.CIO',
    'doc_rep_legal':    '3. C. Rep Legal',
    'doc_cert_banca':   '4. Cert. Bancaria',
    'doc_cert_comerc':  '5. Cert. Comercial',
    'doc_composicion':  '6. Composición Accionaria / Certificado',
    'doc_registro':     '7. Registro',
    'doc_trat_datos':   '8. Autori. Trat. Datos',
    'doc_aviso_priv':   '9. Aviso de Privacidad',
    'doc_basc':         '10. BASC o Equivalente',
    'doc_acuerdo_seg':  '10.1 Acuerdo Seguridad',
    'doc_codigo_etica': '11. Divulgación Código de Ética',
    'doc_risk':         '12. RISK / Compliance',
}
TOTAL_DOCS = len(DOCUMENTOS)


# ==================== BASE DE DATOS ====================
class DatabaseManager:
    def __init__(self):
        self.db_url = SUPABASE_DB_URL
        self.init_database()

    def get_connection(self):
        return psycopg2.connect(self.db_url)

    def init_database(self):
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            cur.execute('''
                CREATE TABLE IF NOT EXISTS proveedores (
                    id SERIAL PRIMARY KEY,
                    fecha_registro TEXT,
                    nombre TEXT NOT NULL,
                    tipo_bien_servicio TEXT,
                    tipo_actividad TEXT,
                    direccion_ciudad TEXT,
                    telefono TEXT,
                    contacto TEXT,
                    correo TEXT,
                    doc_rut INTEGER DEFAULT 0,
                    doc_ccio INTEGER DEFAULT 0,
                    doc_rep_legal INTEGER DEFAULT 0,
                    doc_cert_banca INTEGER DEFAULT 0,
                    doc_cert_comerc INTEGER DEFAULT 0,
                    doc_composicion INTEGER DEFAULT 0,
                    doc_registro INTEGER DEFAULT 0,
                    doc_trat_datos INTEGER DEFAULT 0,
                    doc_aviso_priv INTEGER DEFAULT 0,
                    doc_basc INTEGER DEFAULT 0,
                    doc_acuerdo_seg INTEGER DEFAULT 0,
                    doc_codigo_etica INTEGER DEFAULT 0,
                    doc_risk INTEGER DEFAULT 0,
                    fecha_vinculacion TEXT,
                    ultima_actualizacion TEXT,
                    proxima_actualizacion TEXT,
                    eval_inicial_fecha TEXT,
                    eval_inicial_riesgo TEXT,
                    reevaluacion TEXT,
                    control_visitas TEXT,
                    envio_retroalimentacion TEXT,
                    otros_documentos TEXT
                )
            ''')

            cur.execute('''
                CREATE TABLE IF NOT EXISTS documentos_pdf (
                    id SERIAL PRIMARY KEY,
                    proveedor_id INTEGER NOT NULL REFERENCES proveedores(id) ON DELETE CASCADE,
                    doc_key TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    filesize INTEGER,
                    contenido BYTEA NOT NULL,
                    subido_en TEXT NOT NULL
                )
            ''')

            for col_def in [
                "ADD COLUMN IF NOT EXISTS tipo_actividad TEXT",
                "ADD COLUMN IF NOT EXISTS fecha_vinculacion TEXT",
            ]:
                try:
                    cur.execute(f"ALTER TABLE proveedores {col_def}")
                except Exception:
                    pass

            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Error inicializando base de datos: {e}")

    def guardar_proveedor(self, datos):
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            hora_col = datetime.now() - timedelta(hours=5)
            fecha_actual = hora_col.strftime('%Y-%m-%d %H:%M:%S')
            cur.execute('''
                INSERT INTO proveedores (
                    fecha_registro, nombre, tipo_bien_servicio, tipo_actividad,
                    direccion_ciudad, telefono, contacto, correo,
                    doc_rut, doc_ccio, doc_rep_legal, doc_cert_banca, doc_cert_comerc,
                    doc_composicion, doc_registro, doc_trat_datos, doc_aviso_priv,
                    doc_basc, doc_acuerdo_seg, doc_codigo_etica, doc_risk,
                    fecha_vinculacion, ultima_actualizacion, proxima_actualizacion,
                    eval_inicial_fecha, eval_inicial_riesgo,
                    reevaluacion, control_visitas, envio_retroalimentacion, otros_documentos
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s
                ) RETURNING id
            ''', (
                fecha_actual,
                datos['nombre'], datos['tipo_bien_servicio'], datos.get('tipo_actividad', ''),
                datos['direccion_ciudad'], datos['telefono'], datos['contacto'], datos['correo'],
                datos['doc_rut'], datos['doc_ccio'], datos['doc_rep_legal'],
                datos['doc_cert_banca'], datos['doc_cert_comerc'], datos['doc_composicion'],
                datos['doc_registro'], datos['doc_trat_datos'], datos['doc_aviso_priv'],
                datos['doc_basc'], datos['doc_acuerdo_seg'], datos['doc_codigo_etica'],
                datos['doc_risk'],
                datos.get('fecha_vinculacion', ''),
                datos['ultima_actualizacion'], datos['proxima_actualizacion'],
                datos['eval_inicial_fecha'], datos['eval_inicial_riesgo'],
                datos['reevaluacion'], datos['control_visitas'],
                datos['envio_retroalimentacion'], datos['otros_documentos'],
            ))
            result = cur.fetchone()
            conn.commit()
            conn.close()
            return result[0] if result else None
        except Exception as e:
            st.error(f"Error guardando proveedor: {e}")
            return None

    def actualizar_proveedor(self, proveedor_id, datos):
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute('''
                UPDATE proveedores SET
                    nombre=%s, tipo_bien_servicio=%s, tipo_actividad=%s,
                    direccion_ciudad=%s, telefono=%s, contacto=%s, correo=%s,
                    doc_rut=%s, doc_ccio=%s, doc_rep_legal=%s, doc_cert_banca=%s,
                    doc_cert_comerc=%s, doc_composicion=%s, doc_registro=%s,
                    doc_trat_datos=%s, doc_aviso_priv=%s, doc_basc=%s,
                    doc_acuerdo_seg=%s, doc_codigo_etica=%s, doc_risk=%s,
                    fecha_vinculacion=%s, ultima_actualizacion=%s, proxima_actualizacion=%s,
                    eval_inicial_fecha=%s, eval_inicial_riesgo=%s,
                    reevaluacion=%s, control_visitas=%s,
                    envio_retroalimentacion=%s, otros_documentos=%s
                WHERE id=%s
            ''', (
                datos['nombre'], datos['tipo_bien_servicio'], datos.get('tipo_actividad', ''),
                datos['direccion_ciudad'], datos['telefono'], datos['contacto'], datos['correo'],
                datos['doc_rut'], datos['doc_ccio'], datos['doc_rep_legal'],
                datos['doc_cert_banca'], datos['doc_cert_comerc'], datos['doc_composicion'],
                datos['doc_registro'], datos['doc_trat_datos'], datos['doc_aviso_priv'],
                datos['doc_basc'], datos['doc_acuerdo_seg'], datos['doc_codigo_etica'],
                datos['doc_risk'],
                datos.get('fecha_vinculacion', ''),
                datos['ultima_actualizacion'], datos['proxima_actualizacion'],
                datos['eval_inicial_fecha'], datos['eval_inicial_riesgo'],
                datos['reevaluacion'], datos['control_visitas'],
                datos['envio_retroalimentacion'], datos['otros_documentos'],
                proveedor_id,
            ))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Error actualizando: {e}")
            return False

    def obtener_proveedores(self):
        try:
            conn = self.get_connection()
            df = pd.read_sql_query("SELECT * FROM proveedores ORDER BY nombre", conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Error obteniendo proveedores: {e}")
            return pd.DataFrame()

    def eliminar_proveedor(self, proveedor_id):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM proveedores WHERE id = %s", (proveedor_id,))
        conn.commit()
        conn.close()

    def subir_pdf(self, proveedor_id: int, doc_key: str, filename: str, contenido: bytes):
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            hora_col = datetime.now() - timedelta(hours=5)
            subido_en = hora_col.strftime('%Y-%m-%d %H:%M:%S')
            cur.execute('''
                INSERT INTO documentos_pdf (proveedor_id, doc_key, filename, filesize, contenido, subido_en)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            ''', (proveedor_id, doc_key, filename, len(contenido),
                  psycopg2.Binary(contenido), subido_en))
            new_id = cur.fetchone()[0]
            conn.commit()
            conn.close()
            return new_id
        except Exception as e:
            st.error(f"Error subiendo PDF: {e}")
            return None

    def listar_versiones(self, proveedor_id: int, doc_key: str):
        try:
            conn = self.get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute('''
                SELECT id, filename, filesize, subido_en
                FROM documentos_pdf
                WHERE proveedor_id = %s AND doc_key = %s
                ORDER BY subido_en DESC
            ''', (proveedor_id, doc_key))
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception:
            return []

    def descargar_pdf(self, pdf_id: int):
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT filename, contenido FROM documentos_pdf WHERE id = %s", (pdf_id,))
            row = cur.fetchone()
            conn.close()
            if row:
                return row[0], bytes(row[1])
            return None, None
        except Exception as e:
            st.error(f"Error descargando: {e}")
            return None, None

    def eliminar_version_pdf(self, pdf_id: int):
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("DELETE FROM documentos_pdf WHERE id = %s", (pdf_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Error eliminando versión: {e}")


# ==================== FUNCIONES AUXILIARES ====================
def calcular_indice(row):
    entregados = sum(1 for col in DOCUMENTOS if int(row.get(col) or 0) == 1)
    return round((entregados / TOTAL_DOCS) * 100, 1)

def color_indice(pct):
    return "🟢" if pct >= 80 else "🟡" if pct >= 50 else "🔴"

def estado_texto(pct):
    return "COMPLETO" if pct >= 80 else "EN PROCESO" if pct >= 50 else "CRÍTICO"

def fmt_bytes(size):
    if not size: return ""
    if size < 1024:        return f"{size} B"
    if size < 1_048_576:   return f"{size/1024:.1f} KB"
    return f"{size/1_048_576:.1f} MB"

def _parse_fecha(valor):
    """Intenta parsear una fecha desde texto; retorna datetime o None."""
    if not valor or str(valor).strip() in ('', 'None', 'nan'):
        return None
    s = str(valor).strip()
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%d-%m-%Y'):
        try:
            return datetime.strptime(s[:10], fmt[:10])
        except ValueError:
            continue
    return None

def _dias_diferencia(fecha_str):
    """Días entre hoy y una fecha texto. Negativo = vencida."""
    dt = _parse_fecha(fecha_str)
    if dt is None:
        return None
    return (dt - datetime.now()).days


# ==================== WIDGET PDF POR DOCUMENTO ====================
def widget_documento_pdf(db: DatabaseManager, proveedor_id: int,
                          doc_key: str, doc_label: str,
                          checked: bool, form_key_prefix: str) -> int:
    col_chk, col_body = st.columns([1, 9])
    with col_chk:
        entregado = st.checkbox(
            "✔", value=checked,
            key=f"{form_key_prefix}_{doc_key}_chk",
            label_visibility="collapsed",
        )
    with col_body:
        icon = "✅" if entregado else "📄"
        with st.expander(f"{icon}  {doc_label}", expanded=False):

            st.markdown("##### 📤 Subir nueva versión")
            uploaded = st.file_uploader(
                "Selecciona PDF", type=["pdf"],
                key=f"{form_key_prefix}_{doc_key}_uploader",
                label_visibility="collapsed",
            )
            if uploaded:
                if st.button("💾 Guardar esta versión",
                             key=f"{form_key_prefix}_{doc_key}_btn"):
                    contenido = uploaded.read()
                    new_id = db.subir_pdf(proveedor_id, doc_key, uploaded.name, contenido)
                    if new_id:
                        st.success(f"✅ **{uploaded.name}** guardado ({fmt_bytes(len(contenido))})")
                        st.rerun()

            versiones = db.listar_versiones(proveedor_id, doc_key)
            if versiones:
                st.markdown(f"##### 📂 Historial — {len(versiones)} versión(es)")
                for v in versiones:
                    vc1, vc2, vc3, vc4 = st.columns([4, 2, 2, 1])
                    with vc1:
                        st.markdown(f"🗂 `{v['filename']}`")
                        st.caption(v['subido_en'])
                    with vc2:
                        st.caption(fmt_bytes(v['filesize']))
                    with vc3:
                        fname, fbytes = db.descargar_pdf(v['id'])
                        if fbytes:
                            st.download_button(
                                label="⬇️ Descargar", data=fbytes,
                                file_name=fname, mime="application/pdf",
                                key=f"dl_{v['id']}",
                            )
                    with vc4:
                        if st.button("🗑️", key=f"delpdf_{v['id']}", help="Eliminar esta versión"):
                            db.eliminar_version_pdf(v['id'])
                            st.rerun()
            else:
                st.caption("📭 Sin archivos subidos aún")

    return 1 if entregado else 0


# ==================== GENERADOR EXCEL ====================
def generar_excel_proveedores(df):
    output = io.BytesIO()
    wb = Workbook()

    # ── Estilos compartidos ──────────────────────────────────────────────────
    h_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    h_font = Font(color="FFFFFF", bold=True, size=11)
    verde  = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    rojo   = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    amari  = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    azul_c = PatternFill(start_color="DDEEFF", end_color="DDEEFF", fill_type="solid")
    gris_c = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
    borde  = Border(left=Side(style='thin'), right=Side(style='thin'),
                    top=Side(style='thin'),  bottom=Side(style='thin'))
    centro = Alignment(horizontal='center', vertical='center')
    wrap_c = Alignment(horizontal='center', vertical='center', wrap_text=True)

    def hdr(ws, row, col, value):
        cell = ws.cell(row=row, column=col, value=value)
        cell.font = h_font; cell.fill = h_fill
        cell.alignment = wrap_c; cell.border = borde
        return cell

    # ════════════════════════════════════════════════════════════════════════
    # Hoja 1 – Directorio
    # ════════════════════════════════════════════════════════════════════════
    ws1 = wb.active
    ws1.title = "Directorio Proveedores"
    ws1.merge_cells('A1:H1')
    ws1['A1'] = "DIRECTORIO DE PROVEEDORES"
    ws1['A1'].font = Font(size=15, bold=True, color="1F4E78")
    ws1['A1'].alignment = centro
    ws1.row_dimensions[1].height = 30
    ws1.merge_cells('A2:H2')
    ws1['A2'] = f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}   |   Total: {len(df)}"
    ws1['A2'].alignment = centro
    ws1['A2'].font = Font(italic=True, color="555555")
    for c, h in enumerate(['Nombre Proveedor', 'Tipo Bien / Servicio', 'Tipo Actividad',
                            'Dirección / Ciudad', 'Teléfono', 'Contacto', 'Correo', 'Fecha Registro'], 1):
        hdr(ws1, 4, c, h)
    ws1.row_dimensions[4].height = 20
    for r, (_, row) in enumerate(df.iterrows(), 5):
        for c, f in enumerate(['nombre', 'tipo_bien_servicio', 'tipo_actividad',
                                'direccion_ciudad', 'telefono', 'contacto', 'correo', 'fecha_registro'], 1):
            cell = ws1.cell(row=r, column=c, value=str(row.get(f, '') or ''))
            cell.border = borde
            cell.fill = azul_c if r % 2 == 0 else PatternFill()
    for col, w in zip(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'], [35, 25, 22, 28, 18, 25, 30, 20]):
        ws1.column_dimensions[col].width = w

    # ════════════════════════════════════════════════════════════════════════
    # Hoja 2 – Documentos y Cumplimiento
    # ════════════════════════════════════════════════════════════════════════
    ws2 = wb.create_sheet("Documentos y Cumplimiento")
    total_cols = 2 + TOTAL_DOCS + 3
    ws2.merge_cells(f'A1:{get_column_letter(total_cols)}1')
    ws2['A1'] = "ESTADO DOCUMENTAL POR PROVEEDOR"
    ws2['A1'].font = Font(size=15, bold=True, color="1F4E78")
    ws2['A1'].alignment = centro
    hdrs2 = (['Proveedor', '% Índice'] + list(DOCUMENTOS.values()) +
             ['Fecha Vinculación', 'Última Actualización', 'Próxima Actualización'])
    for c, h in enumerate(hdrs2, 1):
        hdr(ws2, 3, c, h)
    ws2.row_dimensions[3].height = 55
    for r, (_, row) in enumerate(df.iterrows(), 4):
        ind = calcular_indice(row)
        c1 = ws2.cell(row=r, column=1, value=str(row.get('nombre', '')))
        c1.border = borde; c1.font = Font(bold=True)
        c2 = ws2.cell(row=r, column=2, value=f"{ind}%")
        c2.alignment = centro; c2.border = borde; c2.font = Font(bold=True)
        c2.fill = verde if ind >= 80 else amari if ind >= 50 else rojo
        for ci, key in enumerate(DOCUMENTOS.keys(), 3):
            val = int(row.get(key) or 0)
            cell = ws2.cell(row=r, column=ci, value="✓ SÍ" if val else "✗ NO")
            cell.alignment = centro; cell.border = borde
            cell.fill = verde if val else rojo
        col_ua = 3 + TOTAL_DOCS
        ws2.cell(row=r, column=col_ua,   value=str(row.get('fecha_vinculacion', '') or '')).border = borde
        ws2.cell(row=r, column=col_ua+1, value=str(row.get('ultima_actualizacion', '') or '')).border = borde
        ws2.cell(row=r, column=col_ua+2, value=str(row.get('proxima_actualizacion', '') or '')).border = borde
    ws2.column_dimensions['A'].width = 32
    ws2.column_dimensions['B'].width = 14
    for i in range(3, total_cols + 1):
        ws2.column_dimensions[get_column_letter(i)].width = 13

    # ════════════════════════════════════════════════════════════════════════
    # Hoja 3 – Evaluaciones y Control
    # ════════════════════════════════════════════════════════════════════════
    ws3 = wb.create_sheet("Evaluaciones y Control")
    ws3.merge_cells('A1:G1')
    ws3['A1'] = "EVALUACIONES Y CONTROL DE PROVEEDORES"
    ws3['A1'].font = Font(size=15, bold=True, color="1F4E78")
    ws3['A1'].alignment = centro
    ws3.row_dimensions[1].height = 30
    for c, h in enumerate(['Proveedor', '13. Eval. Inicial (Riesgo)', '13. Fecha Eval.',
                            '14. Reevaluación', '15. Control Visitas',
                            '16. Retroalimentación', '17. Otros Docs'], 1):
        hdr(ws3, 3, c, h)
    ws3.row_dimensions[3].height = 40
    riesgo_color = {'ALTO': rojo, 'MEDIO': amari, 'BAJO': verde}
    for r, (_, row) in enumerate(df.iterrows(), 4):
        riesgo = str(row.get('eval_inicial_riesgo', '') or '')
        for ci, v in enumerate([row.get('nombre', ''), riesgo,
                                  row.get('eval_inicial_fecha', ''), row.get('reevaluacion', ''),
                                  row.get('control_visitas', ''), row.get('envio_retroalimentacion', ''),
                                  row.get('otros_documentos', '')], 1):
            cell = ws3.cell(row=r, column=ci, value=str(v) if v else '')
            cell.border = borde
            if ci == 2 and riesgo in riesgo_color:
                cell.fill = riesgo_color[riesgo]
                cell.font = Font(bold=True)
                cell.alignment = centro
    for col, w in zip(['A', 'B', 'C', 'D', 'E', 'F', 'G'], [32, 20, 18, 24, 24, 24, 28]):
        ws3.column_dimensions[col].width = w

    # ════════════════════════════════════════════════════════════════════════
    # Hoja 4 – Informe Ejecutivo
    # ════════════════════════════════════════════════════════════════════════
    ws4 = wb.create_sheet("Informe Ejecutivo")
    ws4.merge_cells('A1:F1')
    ws4['A1'] = "INFORME EJECUTIVO — GESTIÓN DE PROVEEDORES"
    ws4['A1'].font = Font(size=16, bold=True, color="FFFFFF")
    ws4['A1'].fill = h_fill; ws4['A1'].alignment = centro
    ws4.row_dimensions[1].height = 35
    ws4.merge_cells('A2:F2')
    ws4['A2'] = f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}   |   Total: {len(df)}"
    ws4['A2'].alignment = centro; ws4['A2'].font = Font(italic=True, color="444444")

    indices_list  = [calcular_indice(row) for _, row in df.iterrows()]
    prom_indice   = sum(indices_list) / len(indices_list) if indices_list else 0
    n_criticos    = sum(1 for i in indices_list if i < 50)
    n_proceso     = sum(1 for i in indices_list if 50 <= i < 80)
    n_completos   = sum(1 for i in indices_list if i >= 80)
    pct_completos = round(n_completos / len(df) * 100, 1) if len(df) > 0 else 0

    ws4.cell(row=4, column=1).value = "INDICADORES CLAVE DE DESEMPEÑO"
    ws4.cell(row=4, column=1).font = Font(bold=True, size=12, color="1F4E78")
    ws4.merge_cells('A4:F4'); ws4.row_dimensions[4].height = 22

    for i, (label, valor, nivel) in enumerate([
        ("Total Proveedores Registrados",            len(df),               None),
        ("Índice Promedio de Cumplimiento",          f"{prom_indice:.1f}%", prom_indice),
        ("Proveedores Completos (≥ 80%)",            n_completos,           100),
        ("Proveedores En Proceso (50–79%)",          n_proceso,             50),
        ("Proveedores Críticos (< 50%)",             n_criticos,            0),
        ("% Proveedores Completamente Certificados", f"{pct_completos:.1f}%", pct_completos),
    ], 5):
        cl = ws4.cell(row=i, column=1, value=label)
        cl.font = Font(bold=True); cl.border = borde
        cl.fill = PatternFill(start_color="DDEEFF", end_color="DDEEFF", fill_type="solid")
        ws4.merge_cells(f'A{i}:D{i}')
        cv = ws4.cell(row=i, column=5, value=valor)
        cv.alignment = centro; cv.border = borde; cv.font = Font(bold=True, size=12)
        if nivel is not None:
            cv.fill = verde if nivel >= 80 else amari if nivel >= 50 else rojo
        ws4.merge_cells(f'E{i}:F{i}')

    row_rank = 12
    ws4.cell(row=row_rank, column=1).value = "RANKING DE CUMPLIMIENTO"
    ws4.cell(row=row_rank, column=1).font = Font(bold=True, size=12, color="1F4E78")
    ws4.merge_cells(f'A{row_rank}:F{row_rank}')
    for c, h in enumerate(['#', 'Proveedor', 'Tipo Bien', '% Cumplimiento', 'Estado', 'Docs'], 1):
        hdr(ws4, row_rank + 1, c, h)

    df_rk = df.copy()
    df_rk['_idx'] = indices_list
    df_rk = df_rk.sort_values('_idx', ascending=False).reset_index(drop=True)
    for ri, (_, row) in enumerate(df_rk.iterrows(), row_rank + 2):
        ind = row['_idx']
        docs_ok = sum(1 for k in DOCUMENTOS if int(row.get(k) or 0) == 1)
        estado = "✅ COMPLETO" if ind >= 80 else "⚠️ EN PROCESO" if ind >= 50 else "❌ CRÍTICO"
        for ci, v in enumerate([ri - row_rank - 1, row.get('nombre', ''),
                                  row.get('tipo_bien_servicio', ''),
                                  f"{ind}%", estado, f"{docs_ok}/{TOTAL_DOCS}"], 1):
            cell = ws4.cell(row=ri, column=ci, value=v)
            cell.border = borde
            if ci == 4:
                cell.alignment = centro; cell.font = Font(bold=True)
                cell.fill = verde if ind >= 80 else amari if ind >= 50 else rojo
            elif ci == 5:
                cell.alignment = centro

    row_doc = row_rank + len(df_rk) + 4
    ws4.cell(row=row_doc, column=1).value = "ANÁLISIS DE ENTREGA POR DOCUMENTO"
    ws4.cell(row=row_doc, column=1).font = Font(bold=True, size=12, color="1F4E78")
    ws4.merge_cells(f'A{row_doc}:F{row_doc}')
    for c, h in enumerate(['Documento', 'Con Doc.', 'Total', '% Entrega', 'Faltantes'], 1):
        hdr(ws4, row_doc + 1, c, h)
    for di, (key, label) in enumerate(DOCUMENTOS.items(), row_doc + 2):
        entregados = int(df[key].sum()) if key in df.columns else 0
        faltantes  = len(df) - entregados
        pct_doc    = round(entregados / len(df) * 100, 1) if len(df) > 0 else 0
        for ci, v in enumerate([label, entregados, len(df), f"{pct_doc}%", faltantes], 1):
            cell = ws4.cell(row=di, column=ci, value=v)
            cell.border = borde
            if ci == 4:
                cell.alignment = centro; cell.font = Font(bold=True)
                cell.fill = verde if pct_doc >= 80 else amari if pct_doc >= 50 else rojo
    for col, w in zip(['A', 'B', 'C', 'D', 'E', 'F'], [38, 22, 18, 16, 18, 18]):
        ws4.column_dimensions[col].width = w

    # ════════════════════════════════════════════════════════════════════════
    # Hoja 5 – Trazabilidad de Actualizaciones  ← NUEVA
    # ════════════════════════════════════════════════════════════════════════
    ws5 = wb.create_sheet("Trazabilidad Actualizaciones")
    total_prov = len(df)

    # Título
    ws5.merge_cells('A1:H1')
    ws5['A1'] = "TRAZABILIDAD DE ACTUALIZACIONES"
    ws5['A1'].font = Font(size=15, bold=True, color="FFFFFF")
    ws5['A1'].fill = h_fill
    ws5['A1'].alignment = centro
    ws5.row_dimensions[1].height = 32

    ws5.merge_cells('A2:H2')
    ws5['A2'] = (f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}   |   "
                 f"Total proveedores: {total_prov}")
    ws5['A2'].alignment = centro
    ws5['A2'].font = Font(italic=True, color="444444")

    # ── Bloque A: Resumen ejecutivo ─────────────────────────────────────────
    ws5.merge_cells('A4:H4')
    ws5['A4'] = "▌ RESUMEN EJECUTIVO DE ACTUALIZACIONES"
    ws5['A4'].font = Font(bold=True, size=12, color="1F4E78")
    ws5['A4'].fill = PatternFill(start_color="DDEEFF", end_color="DDEEFF", fill_type="solid")
    ws5.row_dimensions[4].height = 22

    # Clasificar cada proveedor según días para vencer su próxima actualización
    n_al_dia, n_por_vencer, n_vencidos, n_sin_fecha = 0, 0, 0, 0
    for _, row in df.iterrows():
        dias = _dias_diferencia(row.get('proxima_actualizacion', ''))
        if dias is None:          n_sin_fecha  += 1
        elif dias > 30:           n_al_dia     += 1
        elif dias >= 0:           n_por_vencer += 1
        else:                     n_vencidos   += 1

    pct_al_dia     = round(n_al_dia     / total_prov * 100, 1) if total_prov else 0
    pct_por_vencer = round(n_por_vencer / total_prov * 100, 1) if total_prov else 0
    pct_vencidos   = round(n_vencidos   / total_prov * 100, 1) if total_prov else 0
    pct_sin_fecha  = round(n_sin_fecha  / total_prov * 100, 1) if total_prov else 0

    for c, h in enumerate(['Estado', 'N° Proveedores', '% del Total', 'Barra Visual'], 1):
        hdr(ws5, 5, c, h)

    resumen_rows = [
        ("🟢 Al día (próxima actualización > 30 días)",     n_al_dia,     pct_al_dia,     verde),
        ("🟡 Por vencer (próxima actualización ≤ 30 días)", n_por_vencer, pct_por_vencer, amari),
        ("🔴 Vencidos (fecha ya pasó)",                     n_vencidos,   pct_vencidos,   rojo),
        ("⚪ Sin fecha registrada",                          n_sin_fecha,  pct_sin_fecha,  gris_c),
    ]
    for ri, (estado, cantidad, pct, fill) in enumerate(resumen_rows, 6):
        barra = "■" * int(pct / 5) + "□" * (20 - int(pct / 5))
        for ci, v in enumerate([estado, cantidad, f"{pct}%", barra], 1):
            cell = ws5.cell(row=ri, column=ci, value=v)
            cell.border = borde; cell.fill = fill
            if ci in (2, 3):
                cell.alignment = centro; cell.font = Font(bold=True)

    # Fila total
    for ci, v in enumerate(["TOTAL", total_prov, "100%", ""], 1):
        cell = ws5.cell(row=10, column=ci, value=v)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = h_fill; cell.alignment = centro; cell.border = borde

    # ── Bloque B: Detalle por proveedor ────────────────────────────────────
    ws5.merge_cells('A12:H12')
    ws5['A12'] = "▌ DETALLE POR PROVEEDOR"
    ws5['A12'].font = Font(bold=True, size=12, color="1F4E78")
    ws5['A12'].fill = PatternFill(start_color="DDEEFF", end_color="DDEEFF", fill_type="solid")
    ws5.row_dimensions[12].height = 22

    det_hdrs = ['Proveedor', 'Última Actualización', 'Próxima Actualización',
                'Días para Vencer', 'Estado Vigencia', '% Docs Entregados',
                'N° Actualizaciones', 'Observación']
    for c, h in enumerate(det_hdrs, 1):
        hdr(ws5, 13, c, h)
    ws5.row_dimensions[13].height = 45

    for r, (_, row) in enumerate(df.iterrows(), 14):
        nombre   = str(row.get('nombre', '') or '')
        ult_act  = str(row.get('ultima_actualizacion', '') or '').strip()
        prox_act = str(row.get('proxima_actualizacion', '') or '').strip()
        indice   = calcular_indice(row)
        dias     = _dias_diferencia(prox_act)
        tiene_ult = ult_act not in ('', 'None', 'nan')
        n_act     = 1 if tiene_ult else 0

        if dias is None:
            estado_vig = "⚪ Sin fecha";          fill_est = gris_c
        elif dias > 30:
            estado_vig = "🟢 Al día";             fill_est = verde
        elif dias >= 0:
            estado_vig = f"🟡 Vence en {dias}d";  fill_est = amari
        else:
            estado_vig = f"🔴 Vencido ({abs(dias)}d)"; fill_est = rojo

        if dias is None:
            obs = "Sin fechas — requiere ingreso"
        elif dias < 0:
            obs = f"Vencido hace {abs(dias)} días — actualización urgente"
        elif dias <= 30:
            obs = f"Vence en {dias} días — programar actualización"
        else:
            obs = "Vigente"

        vals = [
            nombre,
            ult_act  if tiene_ult                           else "—",
            prox_act if prox_act not in ('', 'None', 'nan') else "—",
            dias     if dias is not None                    else "—",
            estado_vig,
            f"{indice}%",
            n_act,
            obs,
        ]
        for c, v in enumerate(vals, 1):
            cell = ws5.cell(row=r, column=c, value=v)
            cell.border = borde
            if c == 4 and isinstance(v, int):
                cell.alignment = centro; cell.font = Font(bold=True)
                cell.fill = verde if v > 30 else amari if v >= 0 else rojo
            elif c == 5:
                cell.fill = fill_est; cell.alignment = centro; cell.font = Font(bold=True)
            elif c == 6:
                cell.alignment = centro; cell.font = Font(bold=True)
                cell.fill = verde if indice >= 80 else amari if indice >= 50 else rojo
            elif c == 7:
                cell.alignment = centro; cell.font = Font(bold=True)
                cell.fill = verde if n_act >= 1 else rojo
            elif r % 2 == 0 and c not in (4, 5, 6, 7):
                cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")

    # ── Bloque C: Estadísticas generales ───────────────────────────────────
    sep = 13 + len(df) + 2
    ws5.merge_cells(f'A{sep}:H{sep}')
    ws5[f'A{sep}'] = "▌ ESTADÍSTICAS GENERALES"
    ws5[f'A{sep}'].font = Font(bold=True, size=12, color="1F4E78")
    ws5[f'A{sep}'].fill = PatternFill(start_color="DDEEFF", end_color="DDEEFF", fill_type="solid")
    ws5.row_dimensions[sep].height = 22

    tiene_ultima  = sum(1 for _, r in df.iterrows()
                        if str(r.get('ultima_actualizacion', '') or '').strip()
                        not in ('', 'None', 'nan'))
    tiene_proxima = sum(1 for _, r in df.iterrows()
                        if str(r.get('proxima_actualizacion', '') or '').strip()
                        not in ('', 'None', 'nan'))
    pct_ult  = round(tiene_ultima  / total_prov * 100, 1) if total_prov else 0
    pct_prox = round(tiene_proxima / total_prov * 100, 1) if total_prov else 0

    for c, h in enumerate(['Indicador', 'Cantidad', '% del Total', 'Observación'], 1):
        hdr(ws5, sep + 1, c, h)

    stats = [
        ("Proveedores con Última Actualización registrada",
         tiene_ultima, pct_ult,
         "Registro histórico presente" if pct_ult == 100 else "Faltan registros"),
        ("Proveedores con Próxima Actualización programada",
         tiene_proxima, pct_prox,
         "Seguimiento programado" if pct_prox == 100 else "Sin programar"),
        ("Proveedores actualizados al menos 1 vez",
         tiene_ultima, pct_ult,
         "≥ 1 actualización documentada"),
        ("Total actualizaciones registradas en el sistema",
         tiene_ultima, "—",
         "Suma de todas las actualizaciones únicas"),
    ]
    for ri, (ind, cant, pct, obs) in enumerate(stats, sep + 2):
        for ci, v in enumerate([ind, cant, f"{pct}%" if isinstance(pct, float) else pct, obs], 1):
            cell = ws5.cell(row=ri, column=ci, value=v)
            cell.border = borde
            if ci == 3 and isinstance(pct, float):
                cell.alignment = centro; cell.font = Font(bold=True)
                cell.fill = verde if pct >= 80 else amari if pct >= 50 else rojo
            if ri % 2 == 0 and ci != 3:
                cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")

    for col, w in zip(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'],
                      [38, 22, 22, 18, 22, 20, 20, 42]):
        ws5.column_dimensions[col].width = w

    wb.save(output)
    output.seek(0)
    return output


# ==================== MAIN ====================
def main():
    st.set_page_config(page_title="Gestión de Proveedores", layout="wide", page_icon="🏢")
    st.title("🏢 Sistema de Gestión de Proveedores")
    st.markdown("**Control Documental, Evaluación y Trazabilidad de Proveedores**")

    if 'db' not in st.session_state:
        with st.spinner("Conectando a la base de datos..."):
            st.session_state.db = DatabaseManager()
    db = st.session_state.db

    tab1, tab2, tab3 = st.tabs([
        "➕ Nuevo Proveedor",
        "📋 Lista de Proveedores",
        "📊 Reportes y Exportación",
    ])

    # ===========================================================
    # TAB 1 – NUEVO PROVEEDOR
    # ===========================================================
    with tab1:
        st.header("Registro de Nuevo Proveedor")

        with st.form("form_proveedor", clear_on_submit=True):
            st.subheader("📌 Información General")
            col1, col2 = st.columns(2)
            with col1:
                nombre             = st.text_input("Nombre del Proveedor *", placeholder="Razón social")
                tipo_bien_servicio = st.text_input("Tipo de Bien / Servicio *",
                                                    placeholder="Ej: Repuestos, Transporte…")
                tipo_actividad     = st.text_input("Tipo de Actividad",
                                                    placeholder="Ej: Fabricante, Distribuidor…")
            with col2:
                direccion_ciudad = st.text_input("Dirección / Ciudad",
                                                  placeholder="Ej: Cra 7 # 10-20, Bogotá")
                telefono = st.text_input("Teléfono / Celular")
                contacto = st.text_input("Contacto")
                correo   = st.text_input("Correo Electrónico")

            st.divider()
            st.subheader("📄 Documentos Solicitados")
            st.info(
                "💡 Marca los documentos recibidos. "
                "Para subir los PDFs, guarda primero el proveedor y luego edítalo desde la lista.",
                icon="ℹ️",
            )
            doc_values = {}
            cols_doc = st.columns(3)
            for idx, (key, label) in enumerate(DOCUMENTOS.items()):
                with cols_doc[idx % 3]:
                    doc_values[key] = 1 if st.checkbox(label, key=f"new_{key}") else 0

            docs_marcados  = sum(doc_values.values())
            indice_preview = round((docs_marcados / TOTAL_DOCS) * 100, 1)
            cp1, cp2 = st.columns(2)
            with cp1:
                st.metric("📊 Índice Documental", f"{indice_preview}%",
                          help=f"{docs_marcados} de {TOTAL_DOCS} marcados")
            with cp2:
                if   indice_preview >= 80: st.success("🟢 COMPLETO")
                elif indice_preview >= 50: st.warning("🟡 EN PROCESO")
                else:                      st.error("🔴 CRÍTICO")

            st.divider()
            st.subheader("📅 Fechas")
            cf1, cf2, cf3 = st.columns(3)
            with cf1: fecha_vinculacion    = st.date_input("📎 Fecha de Vinculación",   value=None)
            with cf2: ultima_actualizacion  = st.date_input("🔄 Última Actualización",   value=None)
            with cf3: proxima_actualizacion = st.date_input("⏭️ Próxima Actualización",  value=None)

            st.divider()
            st.subheader("🔍 Evaluaciones y Control")
            ce1, ce2 = st.columns(2)
            with ce1:
                eval_inicial_fecha  = st.date_input("13. Evaluación Inicial — Fecha", value=None)
                eval_inicial_riesgo = st.selectbox("13. Riesgo del Proveedor",
                                                    ["", "BAJO", "MEDIO", "ALTO"])
                reevaluacion        = st.text_input("14. Reevaluación")
            with ce2:
                control_visitas         = st.text_input("15. Control de Visitas")
                envio_retroalimentacion = st.text_input("16. Envío Retroalimentación")
                otros_documentos        = st.text_area("17. Otros Documentos", height=80)

            st.divider()
            submit = st.form_submit_button("💾 Guardar Proveedor", type="primary")

            if submit:
                if not nombre.strip():
                    st.error("⚠️ El nombre del proveedor es obligatorio.")
                else:
                    datos = {
                        'nombre': nombre.strip().upper(),
                        'tipo_bien_servicio': tipo_bien_servicio,
                        'tipo_actividad': tipo_actividad,
                        'direccion_ciudad': direccion_ciudad,
                        'telefono': telefono, 'contacto': contacto, 'correo': correo,
                        **doc_values,
                        'fecha_vinculacion':     str(fecha_vinculacion)     if fecha_vinculacion    else '',
                        'ultima_actualizacion':  str(ultima_actualizacion)  if ultima_actualizacion  else '',
                        'proxima_actualizacion': str(proxima_actualizacion) if proxima_actualizacion else '',
                        'eval_inicial_fecha':    str(eval_inicial_fecha)    if eval_inicial_fecha    else '',
                        'eval_inicial_riesgo': eval_inicial_riesgo,
                        'reevaluacion': reevaluacion,
                        'control_visitas': control_visitas,
                        'envio_retroalimentacion': envio_retroalimentacion,
                        'otros_documentos': otros_documentos,
                    }
                    prov_id = db.guardar_proveedor(datos)
                    if prov_id:
                        st.success(
                            f"✅ **{datos['nombre']}** guardado (ID: {prov_id})  |  "
                            f"Índice: **{indice_preview}%**"
                        )
                        st.info("📤 Ve a **Lista de Proveedores → Editar** para subir los PDFs.")
                        st.balloons()

    # ===========================================================
    # TAB 2 – LISTA DE PROVEEDORES
    # ===========================================================
    with tab2:
        st.header("📋 Lista de Proveedores")
        if st.button("🔄 Actualizar lista"):
            st.rerun()

        df = db.obtener_proveedores()

        if df.empty:
            st.info("No hay proveedores registrados aún.")
        else:
            indices_all = [calcular_indice(r) for _, r in df.iterrows()]
            prom_all    = sum(indices_all) / len(indices_all) if indices_all else 0

            c1, c2, c3, c4, c5 = st.columns(5)
            with c1: st.metric("Total",          len(df))
            with c2: st.metric("Índice Promedio", f"{prom_all:.1f}%")
            with c3: st.metric("🔴 Críticos",     sum(1 for i in indices_all if i < 50))
            with c4: st.metric("🟡 En Proceso",   sum(1 for i in indices_all if 50 <= i < 80))
            with c5: st.metric("🟢 Completos",    sum(1 for i in indices_all if i >= 80))

            st.divider()

            df_show = df[['id', 'nombre', 'tipo_bien_servicio', 'tipo_actividad',
                           'direccion_ciudad', 'telefono', 'contacto', 'correo']].copy()
            df_show['% Docs'] = [f"{calcular_indice(r):.1f}%" for _, r in df.iterrows()]
            df_show['Estado'] = [
                f"{color_indice(calcular_indice(r))} {estado_texto(calcular_indice(r))}"
                for _, r in df.iterrows()
            ]
            df_show.columns = ['ID', 'Nombre', 'Tipo', 'Actividad', 'Dirección',
                                'Teléfono', 'Contacto', 'Correo', '% Docs', 'Estado']
            st.dataframe(df_show, use_container_width=True, hide_index=True, height=360)

            st.divider()
            st.subheader("🔎 Ver / Editar Proveedor")

            prov_nombre = st.selectbox("Selecciona Proveedor", df['nombre'].tolist())
            row_sel     = df[df['nombre'] == prov_nombre].iloc[0]
            prov_id_sel = int(row_sel['id'])
            indice_sel  = calcular_indice(row_sel)

            m1, m2, m3 = st.columns(3)
            with m1: st.metric("📊 Índice Documental", f"{indice_sel}%")
            with m2:
                docs_ok = sum(1 for k in DOCUMENTOS if int(row_sel.get(k) or 0) == 1)
                st.metric("Docs Entregados", f"{docs_ok} / {TOTAL_DOCS}")
            with m3:
                if   indice_sel >= 80: st.success("🟢 COMPLETO")
                elif indice_sel >= 50: st.warning("🟡 EN PROCESO")
                else:                  st.error("🔴 CRÍTICO")

            with st.expander("📋 Ver información completa"):
                ic1, ic2 = st.columns(2)
                with ic1:
                    st.write(f"**Tipo:** {row_sel.get('tipo_bien_servicio','')}")
                    st.write(f"**Actividad:** {row_sel.get('tipo_actividad','')}")
                    st.write(f"**Dirección:** {row_sel.get('direccion_ciudad','')}")
                    st.write(f"**Teléfono:** {row_sel.get('telefono','')}")
                    st.write(f"**Contacto:** {row_sel.get('contacto','')}")
                    st.write(f"**Correo:** {row_sel.get('correo','')}")
                    st.write(f"**Fecha Vinculación:** {row_sel.get('fecha_vinculacion','')}")
                    st.write(f"**Última Actualización:** {row_sel.get('ultima_actualizacion','')}")
                    st.write(f"**Próxima Actualización:** {row_sel.get('proxima_actualizacion','')}")
                with ic2:
                    riesgo  = row_sel.get('eval_inicial_riesgo', '')
                    color_r = "🔴" if riesgo == "ALTO" else "🟡" if riesgo == "MEDIO" else "🟢" if riesgo == "BAJO" else "⚪"
                    st.write(f"**Evaluación Inicial:** {color_r} {riesgo}")
                    st.write(f"**Fecha Eval.:** {row_sel.get('eval_inicial_fecha','')}")
                    st.write(f"**Reevaluación:** {row_sel.get('reevaluacion','')}")
                    st.write(f"**Control Visitas:** {row_sel.get('control_visitas','')}")
                    st.write(f"**Retroalimentación:** {row_sel.get('envio_retroalimentacion','')}")
                    st.write(f"**Otros Docs:** {row_sel.get('otros_documentos','')}")

            with st.expander("✏️ Editar documentos y subir PDFs"):
                st.markdown(f"### Editando: **{prov_nombre}**")
                st.caption(
                    "Marca ✔ los documentos entregados · "
                    "Sube el PDF de cada uno · "
                    "Descarga o elimina versiones anteriores"
                )

                doc_edit = {}
                for key, label in DOCUMENTOS.items():
                    current = bool(int(row_sel.get(key) or 0))
                    doc_edit[key] = widget_documento_pdf(
                        db=db,
                        proveedor_id=prov_id_sel,
                        doc_key=key,
                        doc_label=label,
                        checked=current,
                        form_key_prefix=f"e{prov_id_sel}",
                    )

                st.divider()
                st.markdown("**📅 Actualizar fechas y datos generales**")

                with st.form(f"form_meta_{prov_id_sel}"):
                    ff1, ff2, ff3 = st.columns(3)
                    with ff1: vinc_e     = st.text_input("Fecha Vinculación",     value=str(row_sel.get('fecha_vinculacion', '') or ''))
                    with ff2: ult_act_e  = st.text_input("Última Actualización",  value=str(row_sel.get('ultima_actualizacion', '') or ''))
                    with ff3: prox_act_e = st.text_input("Próxima Actualización", value=str(row_sel.get('proxima_actualizacion', '') or ''))

                    fr1, fr2 = st.columns(2)
                    with fr1:
                        riesgo_ops = ["", "BAJO", "MEDIO", "ALTO"]
                        riesgo_act = str(row_sel.get('eval_inicial_riesgo', '') or '')
                        riesgo_e   = st.selectbox("Riesgo", riesgo_ops,
                                                   index=riesgo_ops.index(riesgo_act)
                                                   if riesgo_act in riesgo_ops else 0)
                        eval_fech_e = st.text_input("Fecha Eval. Inicial",
                                                     value=str(row_sel.get('eval_inicial_fecha', '') or ''))
                        tipo_act_e  = st.text_input("Tipo de Actividad",
                                                     value=str(row_sel.get('tipo_actividad', '') or ''))
                    with fr2:
                        reeval_e  = st.text_input("Reevaluación",    value=str(row_sel.get('reevaluacion', '') or ''))
                        visitas_e = st.text_input("Control Visitas", value=str(row_sel.get('control_visitas', '') or ''))

                    retro_e = st.text_input("Retroalimentación", value=str(row_sel.get('envio_retroalimentacion', '') or ''))
                    otros_e = st.text_area("Otros Documentos",   value=str(row_sel.get('otros_documentos', '') or ''))

                    if st.form_submit_button("💾 Guardar Cambios", type="primary"):
                        datos_edit = {
                            'nombre':             row_sel['nombre'],
                            'tipo_bien_servicio': row_sel.get('tipo_bien_servicio', ''),
                            'tipo_actividad':     tipo_act_e,
                            'direccion_ciudad':   row_sel.get('direccion_ciudad', ''),
                            'telefono':           row_sel.get('telefono', ''),
                            'contacto':           row_sel.get('contacto', ''),
                            'correo':             row_sel.get('correo', ''),
                            **doc_edit,
                            'fecha_vinculacion':     vinc_e,
                            'ultima_actualizacion':  ult_act_e,
                            'proxima_actualizacion': prox_act_e,
                            'eval_inicial_fecha':    eval_fech_e,
                            'eval_inicial_riesgo':   riesgo_e,
                            'reevaluacion':          reeval_e,
                            'control_visitas':       visitas_e,
                            'envio_retroalimentacion': retro_e,
                            'otros_documentos':      otros_e,
                        }
                        if db.actualizar_proveedor(prov_id_sel, datos_edit):
                            st.success("✅ Proveedor actualizado correctamente")
                            st.rerun()

            st.divider()
            if 'confirmar_eliminar' not in st.session_state:
                st.session_state.confirmar_eliminar = None

            if st.button("🗑️ Eliminar este proveedor", type="secondary"):
                st.session_state.confirmar_eliminar = prov_id_sel

            if st.session_state.get('confirmar_eliminar') == prov_id_sel:
                st.warning(
                    f"⚠️ ¿Seguro que deseas eliminar **{prov_nombre}**? "
                    f"Se eliminarán también todos sus PDFs guardados."
                )
                bc1, bc2 = st.columns(2)
                with bc1:
                    if st.button("✅ Sí, eliminar", type="primary"):
                        db.eliminar_proveedor(prov_id_sel)
                        st.session_state.confirmar_eliminar = None
                        st.success("Proveedor eliminado.")
                        st.rerun()
                with bc2:
                    if st.button("❌ Cancelar"):
                        st.session_state.confirmar_eliminar = None
                        st.rerun()

    # ===========================================================
    # TAB 3 – REPORTES Y EXPORTACIÓN
    # ===========================================================
    with tab3:
        st.header("📊 Reportes y Exportación")
        df = db.obtener_proveedores()

        if df.empty:
            st.info("No hay datos para reportar aún.")
        else:
            indices_rep = [calcular_indice(r) for _, r in df.iterrows()]
            prom_rep    = sum(indices_rep) / len(indices_rep) if indices_rep else 0

            rc1, rc2, rc3, rc4 = st.columns(4)
            with rc1: st.metric("Total",          len(df))
            with rc2: st.metric("Índice Promedio", f"{prom_rep:.1f}%")
            with rc3: st.metric("🔴 Críticos",     sum(1 for i in indices_rep if i < 50))
            with rc4: st.metric("🟢 Completos",    sum(1 for i in indices_rep if i >= 80))

            st.divider()

            df_chart = df[['nombre']].copy()
            df_chart['Índice'] = indices_rep
            df_chart = df_chart.sort_values('Índice', ascending=True)
            fig1 = px.bar(df_chart, x='Índice', y='nombre', orientation='h',
                          title="📊 Índice de Cumplimiento por Proveedor",
                          color='Índice',
                          color_continuous_scale=['#FF4B4B', '#FFC300', '#28B463'],
                          range_color=[0, 100],
                          labels={'Índice': '% Cumplimiento', 'nombre': 'Proveedor'})
            fig1.add_vline(x=80, line_dash="dash", line_color="green",  annotation_text="Meta 80%")
            fig1.add_vline(x=50, line_dash="dash", line_color="orange", annotation_text="Mínimo 50%")
            fig1.update_layout(height=max(300, len(df) * 40))
            st.plotly_chart(fig1, use_container_width=True)

            st.divider()
            doc_pcts = [
                round(int(df[k].sum()) / len(df) * 100, 1) if k in df.columns else 0
                for k in DOCUMENTOS
            ]
            fig2 = px.bar(
                pd.DataFrame({'Documento': list(DOCUMENTOS.values()), '% Entrega': doc_pcts}),
                x='% Entrega', y='Documento', orientation='h',
                title="📄 % de Entrega por Tipo de Documento",
                color='% Entrega',
                color_continuous_scale=['#FF4B4B', '#FFC300', '#28B463'],
                range_color=[0, 100],
            )
            fig2.add_vline(x=80, line_dash="dash", line_color="green")
            fig2.update_layout(height=500)
            st.plotly_chart(fig2, use_container_width=True)

            st.divider()
            if 'eval_inicial_riesgo' in df.columns:
                rc = df['eval_inicial_riesgo'].replace('', 'SIN EVALUAR').value_counts().reset_index()
                rc.columns = ['Riesgo', 'Cantidad']
                fig3 = px.pie(rc, values='Cantidad', names='Riesgo',
                              title="🎯 Distribución de Riesgo",
                              color='Riesgo',
                              color_discrete_map={'ALTO': '#FF4B4B', 'MEDIO': '#FFC300',
                                                  'BAJO': '#28B463', 'SIN EVALUAR': '#AAAAAA'})
                st.plotly_chart(fig3, use_container_width=True)

            st.divider()
            st.subheader("📥 Exportar a Excel")
            st.markdown(
                "5 hojas: **Directorio** · **Documentos y Cumplimiento** · "
                "**Evaluaciones** · **Informe Ejecutivo** · **Trazabilidad Actualizaciones**"
            )
            if st.button("⚙️ Generar Reporte Excel", type="primary"):
                with st.spinner("Generando..."):
                    excel_data = generar_excel_proveedores(df)
                st.download_button(
                    label="📥 Descargar Reporte",
                    data=excel_data,
                    file_name=f"Gestión_Proveedores_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                st.success("✅ Listo para descargar")


if __name__ == "__main__":
    main()
