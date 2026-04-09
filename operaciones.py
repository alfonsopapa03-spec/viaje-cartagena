"""
Sistema de Gestión de Proveedores
Versión 1.0 - Conectado a Supabase (PostgreSQL)
Contexto: Colombia
"""

import streamlit as st
import psycopg2
from datetime import datetime, date, timedelta
import pandas as pd
import io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
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
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS proveedores (
                    id SERIAL PRIMARY KEY,
                    fecha_registro TEXT,
                    nombre TEXT NOT NULL,
                    tipo_bien_servicio TEXT,
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
            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Error inicializando base de datos: {e}")

    def guardar_proveedor(self, datos):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            hora_col = datetime.now() - timedelta(hours=5)
            fecha_actual = hora_col.strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute('''
                INSERT INTO proveedores (
                    fecha_registro, nombre, tipo_bien_servicio, direccion_ciudad,
                    telefono, contacto, correo,
                    doc_rut, doc_ccio, doc_rep_legal, doc_cert_banca, doc_cert_comerc,
                    doc_composicion, doc_registro, doc_trat_datos, doc_aviso_priv,
                    doc_basc, doc_acuerdo_seg, doc_codigo_etica, doc_risk,
                    ultima_actualizacion, proxima_actualizacion,
                    eval_inicial_fecha, eval_inicial_riesgo,
                    reevaluacion, control_visitas, envio_retroalimentacion, otros_documentos
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s
                ) RETURNING id
            ''', (
                fecha_actual,
                datos['nombre'], datos['tipo_bien_servicio'], datos['direccion_ciudad'],
                datos['telefono'], datos['contacto'], datos['correo'],
                datos['doc_rut'], datos['doc_ccio'], datos['doc_rep_legal'],
                datos['doc_cert_banca'], datos['doc_cert_comerc'], datos['doc_composicion'],
                datos['doc_registro'], datos['doc_trat_datos'], datos['doc_aviso_priv'],
                datos['doc_basc'], datos['doc_acuerdo_seg'], datos['doc_codigo_etica'],
                datos['doc_risk'],
                datos['ultima_actualizacion'], datos['proxima_actualizacion'],
                datos['eval_inicial_fecha'], datos['eval_inicial_riesgo'],
                datos['reevaluacion'], datos['control_visitas'],
                datos['envio_retroalimentacion'], datos['otros_documentos']
            ))
            result = cursor.fetchone()
            conn.commit()
            conn.close()
            return result[0] if result else None
        except Exception as e:
            st.error(f"Error guardando proveedor: {e}")
            return None

    def actualizar_proveedor(self, proveedor_id, datos):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE proveedores SET
                    nombre=%s, tipo_bien_servicio=%s, direccion_ciudad=%s,
                    telefono=%s, contacto=%s, correo=%s,
                    doc_rut=%s, doc_ccio=%s, doc_rep_legal=%s, doc_cert_banca=%s,
                    doc_cert_comerc=%s, doc_composicion=%s, doc_registro=%s,
                    doc_trat_datos=%s, doc_aviso_priv=%s, doc_basc=%s,
                    doc_acuerdo_seg=%s, doc_codigo_etica=%s, doc_risk=%s,
                    ultima_actualizacion=%s, proxima_actualizacion=%s,
                    eval_inicial_fecha=%s, eval_inicial_riesgo=%s,
                    reevaluacion=%s, control_visitas=%s,
                    envio_retroalimentacion=%s, otros_documentos=%s
                WHERE id=%s
            ''', (
                datos['nombre'], datos['tipo_bien_servicio'], datos['direccion_ciudad'],
                datos['telefono'], datos['contacto'], datos['correo'],
                datos['doc_rut'], datos['doc_ccio'], datos['doc_rep_legal'],
                datos['doc_cert_banca'], datos['doc_cert_comerc'], datos['doc_composicion'],
                datos['doc_registro'], datos['doc_trat_datos'], datos['doc_aviso_priv'],
                datos['doc_basc'], datos['doc_acuerdo_seg'], datos['doc_codigo_etica'],
                datos['doc_risk'],
                datos['ultima_actualizacion'], datos['proxima_actualizacion'],
                datos['eval_inicial_fecha'], datos['eval_inicial_riesgo'],
                datos['reevaluacion'], datos['control_visitas'],
                datos['envio_retroalimentacion'], datos['otros_documentos'],
                proveedor_id
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
        cursor = conn.cursor()
        cursor.execute("DELETE FROM proveedores WHERE id = %s", (proveedor_id,))
        conn.commit()
        conn.close()


# ==================== FUNCIONES AUXILIARES ====================
def calcular_indice(row):
    """Calcula el % de documentos entregados"""
    doc_cols = list(DOCUMENTOS.keys())
    entregados = sum(1 for col in doc_cols if int(row.get(col, 0)) == 1)
    return round((entregados / TOTAL_DOCS) * 100, 1)


def color_indice(pct):
    if pct >= 80:
        return "🟢"
    elif pct >= 50:
        return "🟡"
    return "🔴"


def estado_texto(pct):
    if pct >= 80:
        return "COMPLETO"
    elif pct >= 50:
        return "EN PROCESO"
    return "CRÍTICO"


# ==================== GENERADOR EXCEL ====================
def generar_excel_proveedores(df):
    output = io.BytesIO()
    wb = Workbook()

    # ----- Estilos globales -----
    h_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    h_font = Font(color="FFFFFF", bold=True, size=11)
    verde  = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    rojo   = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    amari  = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    azul_c = PatternFill(start_color="DDEEFF", end_color="DDEEFF", fill_type="solid")
    borde  = Border(
        left=Side(style='thin'), right=Side(style='thin'),
        top=Side(style='thin'),  bottom=Side(style='thin')
    )
    centro = Alignment(horizontal='center', vertical='center')

    # ============================================================
    # HOJA 1 – DIRECTORIO DE PROVEEDORES
    # ============================================================
    ws1 = wb.active
    ws1.title = "Directorio Proveedores"

    ws1.merge_cells('A1:G1')
    ws1['A1'] = "DIRECTORIO DE PROVEEDORES"
    ws1['A1'].font = Font(size=15, bold=True, color="1F4E78")
    ws1['A1'].alignment = centro
    ws1.row_dimensions[1].height = 30

    ws1.merge_cells('A2:G2')
    ws1['A2'] = f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}   |   Total proveedores: {len(df)}"
    ws1['A2'].alignment = centro
    ws1['A2'].font = Font(italic=True, color="555555")

    hdrs1 = ['Nombre Proveedor', 'Tipo Bien / Servicio', 'Dirección / Ciudad',
             'Teléfono / Celular', 'Contacto', 'Correo Electrónico', 'Fecha Registro']
    for c, h in enumerate(hdrs1, 1):
        cell = ws1.cell(row=4, column=c, value=h)
        cell.font = h_font; cell.fill = h_fill
        cell.alignment = centro; cell.border = borde
    ws1.row_dimensions[4].height = 20

    for r, (_, row) in enumerate(df.iterrows(), 5):
        for c, f in enumerate(['nombre','tipo_bien_servicio','direccion_ciudad',
                                'telefono','contacto','correo','fecha_registro'], 1):
            cell = ws1.cell(row=r, column=c, value=str(row.get(f, '') or ''))
            cell.border = borde
            cell.fill = azul_c if r % 2 == 0 else PatternFill()

    for col, w in zip(['A','B','C','D','E','F','G'], [35,25,28,18,25,30,20]):
        ws1.column_dimensions[col].width = w

    # ============================================================
    # HOJA 2 – ESTADO DOCUMENTAL
    # ============================================================
    ws2 = wb.create_sheet("Documentos y Cumplimiento")
    total_cols = 2 + TOTAL_DOCS + 2   # nombre + % + docs + 2 fechas

    ws2.merge_cells(f'A1:{chr(64+total_cols)}1')
    ws2['A1'] = "ESTADO DOCUMENTAL POR PROVEEDOR"
    ws2['A1'].font = Font(size=15, bold=True, color="1F4E78")
    ws2['A1'].alignment = centro
    ws2.row_dimensions[1].height = 30

    hdrs2 = (['Proveedor', '% Índice Documental']
             + list(DOCUMENTOS.values())
             + ['Última Actualización', 'Próxima Actualización'])
    for c, h in enumerate(hdrs2, 1):
        cell = ws2.cell(row=3, column=c, value=h)
        cell.font = h_font; cell.fill = h_fill
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.border = borde
    ws2.row_dimensions[3].height = 55

    for r, (_, row) in enumerate(df.iterrows(), 4):
        indice = calcular_indice(row)

        # Nombre
        c1 = ws2.cell(row=r, column=1, value=str(row.get('nombre', '')))
        c1.border = borde; c1.font = Font(bold=True)

        # % Índice
        c2 = ws2.cell(row=r, column=2, value=f"{indice}%")
        c2.alignment = centro; c2.border = borde; c2.font = Font(bold=True)
        c2.fill = verde if indice >= 80 else amari if indice >= 50 else rojo

        # Documentos
        for ci, key in enumerate(DOCUMENTOS.keys(), 3):
            val = int(row.get(key, 0))
            cell = ws2.cell(row=r, column=ci, value="✓ SÍ" if val else "✗ NO")
            cell.alignment = centro; cell.border = borde
            cell.fill = verde if val else rojo

        # Fechas
        col_ua = 3 + TOTAL_DOCS
        ws2.cell(row=r, column=col_ua,   value=str(row.get('ultima_actualizacion','') or '')).border = borde
        ws2.cell(row=r, column=col_ua+1, value=str(row.get('proxima_actualizacion','') or '')).border = borde

    ws2.column_dimensions['A'].width = 32
    ws2.column_dimensions['B'].width = 18
    for i in range(3, total_cols + 1):
        letra = chr(64 + i) if i <= 26 else 'A' + chr(64 + i - 26)
        try:
            ws2.column_dimensions[letra].width = 13
        except Exception:
            pass

    # ============================================================
    # HOJA 3 – EVALUACIONES Y CONTROL
    # ============================================================
    ws3 = wb.create_sheet("Evaluaciones y Control")

    ws3.merge_cells('A1:G1')
    ws3['A1'] = "EVALUACIONES Y CONTROL DE PROVEEDORES"
    ws3['A1'].font = Font(size=15, bold=True, color="1F4E78")
    ws3['A1'].alignment = centro
    ws3.row_dimensions[1].height = 30

    hdrs3 = ['Proveedor', '13. Eval. Inicial (Riesgo)', '13. Fecha Eval. Inicial',
             '14. Reevaluación', '15. Control de Visitas',
             '16. Envío Retroalimentación', '17. Otros Documentos']
    for c, h in enumerate(hdrs3, 1):
        cell = ws3.cell(row=3, column=c, value=h)
        cell.font = h_font; cell.fill = h_fill
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.border = borde
    ws3.row_dimensions[3].height = 40

    riesgo_color = {'ALTO': rojo, 'MEDIO': amari, 'BAJO': verde}

    for r, (_, row) in enumerate(df.iterrows(), 4):
        riesgo = str(row.get('eval_inicial_riesgo', '') or '')
        vals = [
            row.get('nombre',''),
            riesgo,
            row.get('eval_inicial_fecha',''),
            row.get('reevaluacion',''),
            row.get('control_visitas',''),
            row.get('envio_retroalimentacion',''),
            row.get('otros_documentos',''),
        ]
        for ci, v in enumerate(vals, 1):
            cell = ws3.cell(row=r, column=ci, value=str(v) if v else '')
            cell.border = borde
            if ci == 2 and riesgo in riesgo_color:
                cell.fill = riesgo_color[riesgo]
                cell.font = Font(bold=True)
                cell.alignment = centro

    for col, w in zip(['A','B','C','D','E','F','G'], [32,20,20,24,24,24,28]):
        ws3.column_dimensions[col].width = w

    # ============================================================
    # HOJA 4 – INFORME EJECUTIVO
    # ============================================================
    ws4 = wb.create_sheet("Informe Ejecutivo")

    # Título grande
    ws4.merge_cells('A1:F1')
    ws4['A1'] = "INFORME EJECUTIVO — GESTIÓN DE PROVEEDORES"
    ws4['A1'].font = Font(size=16, bold=True, color="FFFFFF")
    ws4['A1'].fill = h_fill
    ws4['A1'].alignment = centro
    ws4.row_dimensions[1].height = 35

    ws4.merge_cells('A2:F2')
    ws4['A2'] = f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}   |   Total Proveedores: {len(df)}"
    ws4['A2'].alignment = centro
    ws4['A2'].font = Font(italic=True, color="444444")

    # --- KPIs ---
    indices_list = [calcular_indice(row) for _, row in df.iterrows()]
    prom_indice       = sum(indices_list) / len(indices_list) if indices_list else 0
    n_criticos        = sum(1 for i in indices_list if i < 50)
    n_proceso         = sum(1 for i in indices_list if 50 <= i < 80)
    n_completos       = sum(1 for i in indices_list if i >= 80)
    pct_completos     = round(n_completos / len(df) * 100, 1) if len(df) > 0 else 0

    ws4.cell(row=4, column=1).value = "INDICADORES CLAVE DE DESEMPEÑO"
    ws4.cell(row=4, column=1).font = Font(bold=True, size=12, color="1F4E78")
    ws4.merge_cells('A4:F4')
    ws4.row_dimensions[4].height = 22

    kpis = [
        ("Total de Proveedores Registrados",          len(df),              None),
        ("Índice Promedio de Cumplimiento Documental", f"{prom_indice:.1f}%", prom_indice),
        ("Proveedores Completos (≥ 80%)",              n_completos,          100),
        ("Proveedores En Proceso (50%–79%)",           n_proceso,            50),
        ("Proveedores Críticos (< 50%)",               n_criticos,           0),
        ("% de Proveedores Completamente Certificados",f"{pct_completos:.1f}%", pct_completos),
    ]

    for i, (label, valor, nivel) in enumerate(kpis, 5):
        cell_l = ws4.cell(row=i, column=1, value=label)
        cell_l.font = Font(bold=True); cell_l.border = borde
        cell_l.fill = azul_c
        ws4.merge_cells(f'A{i}:D{i}')

        cell_v = ws4.cell(row=i, column=5, value=valor)
        cell_v.alignment = centro; cell_v.border = borde
        cell_v.font = Font(bold=True, size=12)
        if nivel is not None:
            cell_v.fill = verde if nivel >= 80 else amari if nivel >= 50 else rojo
        ws4.merge_cells(f'E{i}:F{i}')

    # --- Ranking ---
    row_rank = 5 + len(kpis) + 2
    ws4.cell(row=row_rank, column=1).value = "RANKING DE CUMPLIMIENTO DOCUMENTAL"
    ws4.cell(row=row_rank, column=1).font = Font(bold=True, size=12, color="1F4E78")
    ws4.merge_cells(f'A{row_rank}:F{row_rank}')

    rk_headers = ['#', 'Proveedor', 'Tipo Bien / Servicio', '% Cumplimiento', 'Estado', 'Docs Entregados']
    for c, h in enumerate(rk_headers, 1):
        cell = ws4.cell(row=row_rank+1, column=c, value=h)
        cell.font = h_font; cell.fill = h_fill
        cell.alignment = centro; cell.border = borde

    df_rk = df.copy()
    df_rk['_idx'] = indices_list
    df_rk = df_rk.sort_values('_idx', ascending=False).reset_index(drop=True)

    for ri, (_, row) in enumerate(df_rk.iterrows(), row_rank+2):
        ind = row['_idx']
        docs_ok = sum(1 for k in DOCUMENTOS if int(row.get(k, 0)) == 1)
        estado = "✅ COMPLETO" if ind >= 80 else "⚠️ EN PROCESO" if ind >= 50 else "❌ CRÍTICO"

        vals_rk = [ri - row_rank - 1,
                   row.get('nombre',''),
                   row.get('tipo_bien_servicio',''),
                   f"{ind}%",
                   estado,
                   f"{docs_ok} / {TOTAL_DOCS}"]

        for ci, v in enumerate(vals_rk, 1):
            cell = ws4.cell(row=ri, column=ci, value=v)
            cell.border = borde
            if ci == 4:
                cell.alignment = centro; cell.font = Font(bold=True)
                cell.fill = verde if ind >= 80 else amari if ind >= 50 else rojo
            elif ci == 5:
                cell.alignment = centro

    # --- Análisis por documento ---
    row_doc = row_rank + len(df_rk) + 4
    ws4.cell(row=row_doc, column=1).value = "ANÁLISIS DE ENTREGA POR DOCUMENTO"
    ws4.cell(row=row_doc, column=1).font = Font(bold=True, size=12, color="1F4E78")
    ws4.merge_cells(f'A{row_doc}:F{row_doc}')

    doc_hdrs = ['Documento', 'Proveedores con Doc.', 'Total Proveedores', '% Entrega', 'Faltantes']
    for c, h in enumerate(doc_hdrs, 1):
        cell = ws4.cell(row=row_doc+1, column=c, value=h)
        cell.font = h_font; cell.fill = h_fill
        cell.alignment = centro; cell.border = borde

    for di, (key, label) in enumerate(DOCUMENTOS.items(), row_doc+2):
        entregados = int(df[key].sum()) if key in df.columns else 0
        faltantes  = len(df) - entregados
        pct_doc    = round(entregados / len(df) * 100, 1) if len(df) > 0 else 0

        vals_d = [label, entregados, len(df), f"{pct_doc}%", faltantes]
        for ci, v in enumerate(vals_d, 1):
            cell = ws4.cell(row=di, column=ci, value=v)
            cell.border = borde
            if ci == 4:
                cell.alignment = centro; cell.font = Font(bold=True)
                cell.fill = verde if pct_doc >= 80 else amari if pct_doc >= 50 else rojo

    for col, w in zip(['A','B','C','D','E','F'], [38,22,18,16,18,18]):
        ws4.column_dimensions[col].width = w

    wb.save(output)
    output.seek(0)
    return output


# ==================== MAIN ====================
def main():
    st.set_page_config(
        page_title="Gestión de Proveedores",
        layout="wide",
        page_icon="🏢"
    )

    st.title("🏢 Sistema de Gestión de Proveedores")
    st.markdown("**Control Documental, Evaluación y Trazabilidad de Proveedores**")

    if 'db' not in st.session_state:
        with st.spinner("Conectando a la base de datos..."):
            st.session_state.db = DatabaseManager()

    db = st.session_state.db

    tab1, tab2, tab3 = st.tabs([
        "➕ Nuevo Proveedor",
        "📋 Lista de Proveedores",
        "📊 Reportes y Exportación"
    ])

    # ===========================================================
    # TAB 1 – NUEVO PROVEEDOR
    # ===========================================================
    with tab1:
        st.header("Registro de Nuevo Proveedor")

        with st.form("form_proveedor", clear_on_submit=True):

            # ---- Información General ----
            st.subheader("📌 Información General")
            col1, col2 = st.columns(2)
            with col1:
                nombre = st.text_input("Nombre del Proveedor *", placeholder="Razón social o nombre")
                tipo_bien_servicio = st.text_input("Tipo de Bien / Servicio *",
                                                    placeholder="Ej: Repuestos, Transporte, Lubricantes…")
                direccion_ciudad = st.text_input("Dirección / Ciudad",
                                                  placeholder="Ej: Cra 7 # 10-20, Bogotá")
            with col2:
                telefono = st.text_input("Teléfono / Celular", placeholder="Ej: 3001234567")
                contacto = st.text_input("Contacto", placeholder="Nombre de la persona de contacto")
                correo = st.text_input("Correo Electrónico", placeholder="proveedor@empresa.com")

            # ---- Documentos ----
            st.divider()
            st.subheader("📄 Documentos Solicitados")
            st.caption("Marca los documentos que el proveedor ya ha entregado")

            doc_values = {}
            cols_doc = st.columns(3)
            for idx, (key, label) in enumerate(DOCUMENTOS.items()):
                with cols_doc[idx % 3]:
                    doc_values[key] = 1 if st.checkbox(label, key=f"new_{key}") else 0

            # Preview índice en tiempo real
            docs_marcados = sum(doc_values.values())
            indice_preview = round((docs_marcados / TOTAL_DOCS) * 100, 1)
            color_p = color_indice(indice_preview)
            estado_p = estado_texto(indice_preview)

            col_p1, col_p2 = st.columns(2)
            with col_p1:
                st.metric("📊 Índice Documental",
                          f"{indice_preview}%",
                          help=f"{docs_marcados} de {TOTAL_DOCS} documentos marcados")
            with col_p2:
                if indice_preview >= 80:
                    st.success(f"{color_p} Estado: {estado_p}")
                elif indice_preview >= 50:
                    st.warning(f"{color_p} Estado: {estado_p}")
                else:
                    st.error(f"{color_p} Estado: {estado_p}")

            # ---- Fechas ----
            st.divider()
            st.subheader("📅 Fechas de Actualización Documental")
            col1, col2 = st.columns(2)
            with col1:
                ultima_actualizacion = st.date_input("Última Actualización", value=None)
            with col2:
                proxima_actualizacion = st.date_input("Próxima Actualización", value=None)

            # ---- Evaluaciones ----
            st.divider()
            st.subheader("🔍 Evaluaciones y Control")

            col1, col2 = st.columns(2)
            with col1:
                eval_inicial_fecha = st.date_input("13. Evaluación Inicial — Fecha", value=None)
                eval_inicial_riesgo = st.selectbox(
                    "13. Riesgo del Proveedor",
                    ["", "BAJO", "MEDIO", "ALTO"],
                    help="Resultado de la evaluación inicial de riesgo"
                )
                reevaluacion = st.text_input(
                    "14. Reevaluación de Proveedores",
                    placeholder="Fecha o descripción"
                )
            with col2:
                control_visitas = st.text_input(
                    "15. Control de Visitas",
                    placeholder="Fecha o descripción — Revisión acuerdos de seguridad"
                )
                envio_retroalimentacion = st.text_input(
                    "16. Envío Retroalimentación",
                    placeholder="Fecha o descripción"
                )
                otros_documentos = st.text_area(
                    "17. Otros Documentos Proveedores",
                    placeholder="Describe otros documentos adicionales entregados…",
                    height=80
                )

            st.divider()
            submit = st.form_submit_button("💾 Guardar Proveedor", type="primary")

            if submit:
                if not nombre.strip():
                    st.error("⚠️ El nombre del proveedor es obligatorio.")
                else:
                    datos = {
                        'nombre': nombre.strip().upper(),
                        'tipo_bien_servicio': tipo_bien_servicio,
                        'direccion_ciudad': direccion_ciudad,
                        'telefono': telefono,
                        'contacto': contacto,
                        'correo': correo,
                        **doc_values,
                        'ultima_actualizacion': str(ultima_actualizacion) if ultima_actualizacion else '',
                        'proxima_actualizacion': str(proxima_actualizacion) if proxima_actualizacion else '',
                        'eval_inicial_fecha': str(eval_inicial_fecha) if eval_inicial_fecha else '',
                        'eval_inicial_riesgo': eval_inicial_riesgo,
                        'reevaluacion': reevaluacion,
                        'control_visitas': control_visitas,
                        'envio_retroalimentacion': envio_retroalimentacion,
                        'otros_documentos': otros_documentos,
                    }
                    prov_id = db.guardar_proveedor(datos)
                    if prov_id:
                        st.success(
                            f"✅ Proveedor **{datos['nombre']}** guardado exitosamente  "
                            f"(ID: {prov_id})  |  Índice Documental: **{indice_preview}%**"
                        )
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
            st.info("No hay proveedores registrados aún. Registra uno en la pestaña ➕.")
        else:
            # KPIs
            indices_all = [calcular_indice(row) for _, row in df.iterrows()]
            prom_all = sum(indices_all) / len(indices_all) if indices_all else 0

            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Proveedores", len(df))
            with col2:
                st.metric("Índice Promedio", f"{prom_all:.1f}%")
            with col3:
                st.metric("🔴 Críticos < 50%", sum(1 for i in indices_all if i < 50))
            with col4:
                st.metric("🟡 En Proceso", sum(1 for i in indices_all if 50 <= i < 80))
            with col5:
                st.metric("🟢 Completos ≥ 80%", sum(1 for i in indices_all if i >= 80))

            st.divider()

            # Tabla resumen
            df_show = df[['id','nombre','tipo_bien_servicio','direccion_ciudad',
                           'telefono','contacto','correo']].copy()
            df_show['% Docs'] = [f"{calcular_indice(r):.1f}%" for _, r in df.iterrows()]
            df_show['Estado'] = [
                f"{color_indice(calcular_indice(r))} {estado_texto(calcular_indice(r))}"
                for _, r in df.iterrows()
            ]
            df_show.columns = ['ID','Nombre','Tipo','Dirección','Teléfono','Contacto','Correo','% Docs','Estado']
            st.dataframe(df_show, use_container_width=True, hide_index=True, height=380)

            st.divider()
            st.subheader("🔎 Ver / Editar Proveedor")

            nombres_opciones = df['nombre'].tolist()
            prov_nombre = st.selectbox("Selecciona Proveedor", nombres_opciones)
            row_sel = df[df['nombre'] == prov_nombre].iloc[0]
            prov_id_sel = int(row_sel['id'])
            indice_sel = calcular_indice(row_sel)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📊 Índice Documental", f"{indice_sel}%")
            with col2:
                docs_ok = sum(1 for k in DOCUMENTOS if int(row_sel.get(k, 0)) == 1)
                st.metric("Docs Entregados", f"{docs_ok} / {TOTAL_DOCS}")
            with col3:
                if indice_sel >= 80:
                    st.success(f"🟢 COMPLETO")
                elif indice_sel >= 50:
                    st.warning(f"🟡 EN PROCESO")
                else:
                    st.error(f"🔴 CRÍTICO")

            st.markdown("**Estado de Documentos:**")
            d_cols = st.columns(4)
            for idx2, (key, label) in enumerate(DOCUMENTOS.items()):
                with d_cols[idx2 % 4]:
                    if int(row_sel.get(key, 0)) == 1:
                        st.success(f"✅ {label}")
                    else:
                        st.error(f"❌ {label}")

            # Info evaluaciones
            with st.expander("📋 Ver información completa"):
                c1, c2 = st.columns(2)
                with c1:
                    st.write(f"**Tipo:** {row_sel.get('tipo_bien_servicio','')}")
                    st.write(f"**Dirección:** {row_sel.get('direccion_ciudad','')}")
                    st.write(f"**Teléfono:** {row_sel.get('telefono','')}")
                    st.write(f"**Contacto:** {row_sel.get('contacto','')}")
                    st.write(f"**Correo:** {row_sel.get('correo','')}")
                    st.write(f"**Última Actualización:** {row_sel.get('ultima_actualizacion','')}")
                    st.write(f"**Próxima Actualización:** {row_sel.get('proxima_actualizacion','')}")
                with c2:
                    riesgo = row_sel.get('eval_inicial_riesgo','')
                    color_r = "🔴" if riesgo == "ALTO" else "🟡" if riesgo == "MEDIO" else "🟢" if riesgo == "BAJO" else "⚪"
                    st.write(f"**13. Evaluación Inicial:** {color_r} {riesgo}")
                    st.write(f"**13. Fecha Eval. Inicial:** {row_sel.get('eval_inicial_fecha','')}")
                    st.write(f"**14. Reevaluación:** {row_sel.get('reevaluacion','')}")
                    st.write(f"**15. Control Visitas:** {row_sel.get('control_visitas','')}")
                    st.write(f"**16. Retroalimentación:** {row_sel.get('envio_retroalimentacion','')}")
                    st.write(f"**17. Otros Docs:** {row_sel.get('otros_documentos','')}")

            # Edición rápida de documentos
            with st.expander("✏️ Editar documentos del proveedor"):
                with st.form(f"form_editar_{prov_id_sel}"):
                    st.markdown(f"**Editando:** {prov_nombre}")
                    doc_edit = {}
                    e_cols = st.columns(3)
                    for idx3, (key, label) in enumerate(DOCUMENTOS.items()):
                        with e_cols[idx3 % 3]:
                            current = bool(int(row_sel.get(key, 0)))
                            doc_edit[key] = 1 if st.checkbox(label, value=current, key=f"edit_{key}_{prov_id_sel}") else 0

                    col_f1, col_f2 = st.columns(2)
                    with col_f1:
                        ult_act_e = st.text_input("Última Actualización", value=str(row_sel.get('ultima_actualizacion','') or ''))
                    with col_f2:
                        prox_act_e = st.text_input("Próxima Actualización", value=str(row_sel.get('proxima_actualizacion','') or ''))

                    col_r1, col_r2 = st.columns(2)
                    with col_r1:
                        riesgo_opciones = ["", "BAJO", "MEDIO", "ALTO"]
                        riesgo_actual = str(row_sel.get('eval_inicial_riesgo','') or '')
                        riesgo_idx = riesgo_opciones.index(riesgo_actual) if riesgo_actual in riesgo_opciones else 0
                        riesgo_e = st.selectbox("Riesgo Proveedor", riesgo_opciones, index=riesgo_idx)
                        eval_fecha_e = st.text_input("Fecha Evaluación Inicial", value=str(row_sel.get('eval_inicial_fecha','') or ''))
                    with col_r2:
                        reeval_e = st.text_input("Reevaluación", value=str(row_sel.get('reevaluacion','') or ''))
                        visitas_e = st.text_input("Control Visitas", value=str(row_sel.get('control_visitas','') or ''))

                    retro_e = st.text_input("Envío Retroalimentación", value=str(row_sel.get('envio_retroalimentacion','') or ''))
                    otros_e = st.text_area("Otros Documentos", value=str(row_sel.get('otros_documentos','') or ''))

                    guardar_edit = st.form_submit_button("💾 Guardar Cambios", type="primary")

                    if guardar_edit:
                        datos_edit = {
                            'nombre': row_sel['nombre'],
                            'tipo_bien_servicio': row_sel.get('tipo_bien_servicio',''),
                            'direccion_ciudad': row_sel.get('direccion_ciudad',''),
                            'telefono': row_sel.get('telefono',''),
                            'contacto': row_sel.get('contacto',''),
                            'correo': row_sel.get('correo',''),
                            **doc_edit,
                            'ultima_actualizacion': ult_act_e,
                            'proxima_actualizacion': prox_act_e,
                            'eval_inicial_fecha': eval_fecha_e,
                            'eval_inicial_riesgo': riesgo_e,
                            'reevaluacion': reeval_e,
                            'control_visitas': visitas_e,
                            'envio_retroalimentacion': retro_e,
                            'otros_documentos': otros_e,
                        }
                        if db.actualizar_proveedor(prov_id_sel, datos_edit):
                            st.success("✅ Proveedor actualizado correctamente")
                            st.rerun()

            st.divider()
            if st.button("🗑️ Eliminar este proveedor", type="secondary"):
                db.eliminar_proveedor(prov_id_sel)
                st.success(f"Proveedor {prov_nombre} eliminado.")
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
            prom_rep = sum(indices_rep) / len(indices_rep) if indices_rep else 0

            # KPIs
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Proveedores", len(df))
            with col2:
                st.metric("Índice Promedio", f"{prom_rep:.1f}%")
            with col3:
                st.metric("🔴 Críticos", sum(1 for i in indices_rep if i < 50))
            with col4:
                st.metric("🟢 Completos", sum(1 for i in indices_rep if i >= 80))

            st.divider()

            # Gráfico 1 – Cumplimiento por proveedor
            df_chart = df[['nombre']].copy()
            df_chart['Índice'] = indices_rep
            df_chart = df_chart.sort_values('Índice', ascending=True)

            fig1 = px.bar(
                df_chart, x='Índice', y='nombre', orientation='h',
                title="📊 Índice de Cumplimiento Documental por Proveedor",
                color='Índice',
                color_continuous_scale=['#FF4B4B', '#FFC300', '#28B463'],
                range_color=[0, 100],
                labels={'Índice': '% Cumplimiento', 'nombre': 'Proveedor'}
            )
            fig1.add_vline(x=80, line_dash="dash", line_color="green",
                           annotation_text="Meta 80%")
            fig1.add_vline(x=50, line_dash="dash", line_color="orange",
                           annotation_text="Mínimo 50%")
            fig1.update_layout(height=max(300, len(df) * 40))
            st.plotly_chart(fig1, use_container_width=True)

            st.divider()

            # Gráfico 2 – Entrega por tipo de documento
            doc_labels = list(DOCUMENTOS.values())
            doc_pcts = []
            for key in DOCUMENTOS:
                if key in df.columns:
                    entregados = int(df[key].sum())
                    doc_pcts.append(round(entregados / len(df) * 100, 1))
                else:
                    doc_pcts.append(0)

            df_docs_chart = pd.DataFrame({'Documento': doc_labels, '% Entrega': doc_pcts})
            fig2 = px.bar(
                df_docs_chart, x='% Entrega', y='Documento', orientation='h',
                title="📄 % de Entrega por Tipo de Documento",
                color='% Entrega',
                color_continuous_scale=['#FF4B4B', '#FFC300', '#28B463'],
                range_color=[0, 100]
            )
            fig2.add_vline(x=80, line_dash="dash", line_color="green")
            fig2.update_layout(height=500)
            st.plotly_chart(fig2, use_container_width=True)

            st.divider()

            # Gráfico 3 – Distribución de riesgo
            if 'eval_inicial_riesgo' in df.columns:
                riesgo_counts = df['eval_inicial_riesgo'].replace('', 'SIN EVALUAR').value_counts().reset_index()
                riesgo_counts.columns = ['Riesgo', 'Cantidad']
                fig3 = px.pie(riesgo_counts, values='Cantidad', names='Riesgo',
                              title="🎯 Distribución de Riesgo de Proveedores",
                              color='Riesgo',
                              color_discrete_map={'ALTO': '#FF4B4B', 'MEDIO': '#FFC300',
                                                  'BAJO': '#28B463', 'SIN EVALUAR': '#AAAAAA'})
                st.plotly_chart(fig3, use_container_width=True)

            st.divider()
            st.subheader("📥 Exportar Reporte Completo a Excel")
            st.markdown("""
            El archivo Excel incluye **4 hojas detalladas**:
            - 📋 **Directorio Proveedores** — datos generales de contacto
            - 📄 **Documentos y Cumplimiento** — estado documental con semáforo por proveedor
            - 🔍 **Evaluaciones y Control** — evaluación de riesgo, visitas, retroalimentación
            - 📊 **Informe Ejecutivo** — KPIs, ranking, análisis por documento
            """)

            if st.button("⚙️ Generar Reporte Excel", type="primary"):
                with st.spinner("Generando reporte..."):
                    excel_data = generar_excel_proveedores(df)
                fecha_excel = datetime.now().strftime('%Y-%m-%d')
                st.download_button(
                    label="📥 Descargar Reporte Completo",
                    data=excel_data,
                    file_name=f"Gestión_Proveedores_{fecha_excel}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.success("✅ Reporte listo para descargar")


if __name__ == "__main__":
    main()
