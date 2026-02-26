import streamlit as st
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from PIL import Image
import io
import plotly.express as px
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ==================== CONFIGURACIÓN DE PÁGINA ====================
st.set_page_config(
    page_title="Logística Cartagena",
    layout="wide",
    page_icon="🚛",
    initial_sidebar_state="collapsed"
)

# ==================== CREDENCIALES SUPABASE ====================
SUPABASE_DB_URL = "postgresql://postgres.scjqqcrkjdavetdyxtrf:GV69W?B8v$x4wH?@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

# ==================== UNIDADES DE MEDIDA ====================
UNIDADES_MEDIDA = [
    "Toneladas (t)",
    "Kilogramos (kg)",
    "Libras (lb)",
    "Sacos",
    "Unidades",
    "M³ (metros cúbicos)",
]

# Factores de conversión a toneladas para el dashboard
CONVERSION_A_TONELADAS = {
    "Toneladas (t)": 1.0,
    "Kilogramos (kg)": 0.001,
    "Libras (lb)": 0.000453592,
    "Sacos": None,       # No convertible automáticamente
    "Unidades": None,
    "M³ (metros cúbicos)": None,
}

# ==================== TIPOS DE CARGA ====================
TIPOS_CARGA = [
    "Sacos de Arroz",
    "Sacos de Azúcar",
    "Sacos de Café",
    "Sacos de Harina",
    "Fertilizantes",
    "Ladrillos",
    "Cemento",
    "Arena / Gravilla",
    "Carbón",
    "Contenedor",
    "Carga General",
    "Otro"
]

# ==================== GESTOR DE BASE DE DATOS ====================
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
                CREATE TABLE IF NOT EXISTS tractomulas (
                    id SERIAL PRIMARY KEY,
                    placa TEXT UNIQUE NOT NULL,
                    tipo TEXT,
                    conductor TEXT
                )
            ''')
            try:
                cursor.execute("ALTER TABLE tractomulas ADD COLUMN IF NOT EXISTS conductor TEXT")
                conn.commit()
            except:
                conn.rollback()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS operaciones_cartagena (
                    id SERIAL PRIMARY KEY,
                    fecha_registro TIMESTAMP DEFAULT (now() AT TIME ZONE 'America/Bogota'),
                    fecha_operacion DATE NOT NULL,
                    placa TEXT NOT NULL,
                    conductor TEXT,
                    tipo_carga TEXT,
                    unidad_medida TEXT DEFAULT 'Toneladas (t)',
                    descripcion TEXT,
                    cantidad_sacos INTEGER,
                    toneladas REAL,
                    cantidad_texto TEXT,
                    imagen_comprobante BYTEA,
                    nombre_archivo TEXT
                )
            ''')

            # Agregar columnas nuevas si no existen (para bases ya creadas)
            for col_sql in [
                "ALTER TABLE operaciones_cartagena ADD COLUMN IF NOT EXISTS tipo_carga TEXT",
                "ALTER TABLE operaciones_cartagena ADD COLUMN IF NOT EXISTS unidad_medida TEXT DEFAULT 'Toneladas (t)'",
                "ALTER TABLE operaciones_cartagena ADD COLUMN IF NOT EXISTS cantidad_texto TEXT",
            ]:
                try:
                    cursor.execute(col_sql)
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_op_fecha ON operaciones_cartagena(fecha_operacion);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_op_placa ON operaciones_cartagena(placa);")

            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Error DB: {e}")

    def obtener_datos_dashboard(self, fecha_inicio, fecha_fin):
        conn = self.get_connection()
        query = """
            SELECT fecha_operacion, placa, conductor, tipo_carga, unidad_medida, cantidad_sacos, toneladas
            FROM operaciones_cartagena
            WHERE fecha_operacion BETWEEN %s AND %s
            ORDER BY fecha_operacion ASC
        """
        try:
            df = pd.read_sql(query, conn, params=(fecha_inicio, fecha_fin))
            return df
        except:
            return pd.DataFrame()
        finally:
            conn.close()

    def obtener_vehiculos_completo(self):
        conn = self.get_connection()
        try:
            df = pd.read_sql("SELECT placa, conductor, tipo FROM tractomulas ORDER BY placa", conn)
            return df
        except:
            return pd.DataFrame()
        finally:
            conn.close()

    def guardar_vehiculo(self, placa, tipo, conductor):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = """
                INSERT INTO tractomulas (placa, tipo, conductor)
                VALUES (%s, %s, %s)
                ON CONFLICT (placa)
                DO UPDATE SET conductor = EXCLUDED.conductor, tipo = EXCLUDED.tipo
            """
            cursor.execute(sql, (placa, tipo, conductor))
            conn.commit()
            conn.close()
            return True
        except:
            return False

    def eliminar_vehiculo(self, placa):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM tractomulas WHERE placa = %s", (placa,))
            conn.commit()
            conn.close()
            return True
        except:
            return False

    def guardar_operacion(self, fecha, placa, conductor, tipo_carga, unidad_medida, descripcion, sacos, cantidad, cantidad_texto, imagen_bytes, nombre_archivo):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            factor = CONVERSION_A_TONELADAS.get(unidad_medida)
            toneladas = round(cantidad * factor, 4) if factor else None
            sql = '''
                INSERT INTO operaciones_cartagena
                (fecha_operacion, placa, conductor, tipo_carga, unidad_medida, descripcion, cantidad_sacos, toneladas, cantidad_texto, imagen_comprobante, nombre_archivo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            if imagen_bytes:
                imagen_bytes = psycopg2.Binary(imagen_bytes)
            cursor.execute(sql, (fecha, placa, conductor, tipo_carga, unidad_medida, descripcion, sacos, toneladas, cantidad_texto, imagen_bytes, nombre_archivo))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Error guardando: {e}")
            return False

    def actualizar_operacion(self, registro_id, fecha, placa, conductor, tipo_carga, unidad_medida, descripcion, sacos, cantidad, cantidad_texto):
        try:
            factor = CONVERSION_A_TONELADAS.get(unidad_medida)
            toneladas = round(cantidad * factor, 4) if factor else None
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = '''
                UPDATE operaciones_cartagena
                SET fecha_operacion=%s, placa=%s, conductor=%s, tipo_carga=%s, unidad_medida=%s,
                    descripcion=%s, cantidad_sacos=%s, toneladas=%s, cantidad_texto=%s
                WHERE id=%s
            '''
            cursor.execute(sql, (fecha, placa, conductor, tipo_carga, unidad_medida, descripcion, sacos, toneladas, cantidad_texto, registro_id))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Error actualizando: {e}")
            return False
        """Actualiza un registro existente sin tocar la imagen"""
        try:
            factor = CONVERSION_A_TONELADAS.get(unidad_medida)
            toneladas = round(cantidad * factor, 4) if factor else None
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = '''
                UPDATE operaciones_cartagena
                SET fecha_operacion=%s, placa=%s, conductor=%s, tipo_carga=%s, unidad_medida=%s,
                    descripcion=%s, cantidad_sacos=%s, toneladas=%s
                WHERE id=%s
            '''
            cursor.execute(sql, (fecha, placa, conductor, tipo_carga, unidad_medida, descripcion, sacos, toneladas, registro_id))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Error actualizando: {e}")
            return False

    def obtener_historial(self, fecha_inicio=None, fecha_fin=None, placa=None, conductor=None, tipo_carga=None):
        conn = self.get_connection()
        query = """
            SELECT id, fecha_operacion, placa, conductor, tipo_carga, unidad_medida, descripcion,
                   cantidad_sacos, toneladas, cantidad_texto, nombre_archivo
            FROM operaciones_cartagena
            WHERE 1=1
        """
        params = []
        if fecha_inicio:
            query += " AND fecha_operacion >= %s"
            params.append(fecha_inicio)
        if fecha_fin:
            query += " AND fecha_operacion <= %s"
            params.append(fecha_fin)
        if placa and placa != "Todas":
            query += " AND placa = %s"
            params.append(placa)
        if conductor:
            query += " AND conductor ILIKE %s"
            params.append(f"%{conductor}%")
        if tipo_carga and tipo_carga != "Todos":
            query += " AND tipo_carga = %s"
            params.append(tipo_carga)

        query += " ORDER BY fecha_operacion DESC, id DESC"

        try:
            df = pd.read_sql(query, conn, params=params)
            return df
        except:
            return pd.DataFrame()
        finally:
            conn.close()

    def obtener_imagen(self, registro_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT imagen_comprobante FROM operaciones_cartagena WHERE id = %s", (registro_id,))
        result = cursor.fetchone()
        conn.close()
        if result and result[0]:
            return bytes(result[0])
        return None

    def eliminar_registro(self, registro_id):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM operaciones_cartagena WHERE id = %s", (registro_id,))
            conn.commit()
            conn.close()
            return True
        except:
            return False


# ==================== UTILIDADES ====================
def parse_cantidad(texto: str) -> float:
    """
    Convierte texto de cantidad en formato colombiano o internacional a float.
    Ejemplos válidos:
      28.910,00  → 28910.0
      28,910.00  → 28910.0
      28900      → 28900.0
      1.500      → 1500.0
      1,5        → 1.5
    """
    texto = str(texto).strip().replace(" ", "")
    if not texto or texto == "0":
        return 0.0

    tiene_punto = "." in texto
    tiene_coma = "," in texto

    if tiene_punto and tiene_coma:
        # Determinar cuál es separador de miles y cuál decimal
        pos_punto = texto.rfind(".")
        pos_coma = texto.rfind(",")
        if pos_coma > pos_punto:
            # Formato colombiano: 28.910,00
            texto = texto.replace(".", "").replace(",", ".")
        else:
            # Formato inglés: 28,910.00
            texto = texto.replace(",", "")
    elif tiene_coma:
        partes = texto.split(",")
        if len(partes) == 2 and len(partes[1]) <= 2:
            # Es decimal: 28,90 → 28.90
            texto = texto.replace(",", ".")
        else:
            # Es separador de miles: 28,900 → 28900
            texto = texto.replace(",", "")
    elif tiene_punto:
        partes = texto.split(".")
        if len(partes) == 2 and len(partes[1]) <= 2:
            # Es decimal: 28.90
            pass
        else:
            # Es separador de miles: 28.900 → 28900
            texto = texto.replace(".", "")

    try:
        return float(texto)
    except ValueError:
        return 0.0



    if uploaded_file is None:
        return None
    try:
        image = Image.open(uploaded_file)
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
        max_width = 1024
        if image.width > max_width:
            ratio = max_width / image.width
            new_height = int(image.height * ratio)
            image = image.resize((max_width, new_height), Image.Resampling.LANCZOS)
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='JPEG', quality=70)
        return img_byte_arr.getvalue()
    except Exception as e:
        st.error(f"Error imagen: {e}")
        return None


def generar_excel(df: pd.DataFrame, titulo: str = "Informe Operaciones") -> bytes:
    """Genera un Excel profesional con formato a partir de un DataFrame"""
    wb = Workbook()
    ws = wb.active
    ws.title = "Operaciones"

    # Estilos
    color_header = "1F4E79"
    color_subheader = "2E75B6"
    color_alt = "D6E4F0"

    font_titulo = Font(name="Arial", bold=True, size=14, color="FFFFFF")
    font_header = Font(name="Arial", bold=True, size=10, color="FFFFFF")
    font_normal = Font(name="Arial", size=9)
    font_total = Font(name="Arial", bold=True, size=10)

    fill_titulo = PatternFill("solid", start_color=color_header)
    fill_header = PatternFill("solid", start_color=color_subheader)
    fill_alt = PatternFill("solid", start_color=color_alt)
    fill_total = PatternFill("solid", start_color="BDD7EE")

    border_thin = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    align_center = Alignment(horizontal="center", vertical="center")
    align_left = Alignment(horizontal="left", vertical="center")

    # --- FILA TÍTULO ---
    ws.merge_cells("A1:I1")
    ws["A1"] = f"🚛 {titulo}"
    ws["A1"].font = font_titulo
    ws["A1"].fill = fill_titulo
    ws["A1"].alignment = align_center
    ws.row_dimensions[1].height = 28

    # --- FILA FECHA GENERACIÓN ---
    ws.merge_cells("A2:I2")
    ws["A2"] = f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}  |  Total registros: {len(df)}"
    ws["A2"].font = Font(name="Arial", italic=True, size=9, color="555555")
    ws["A2"].alignment = align_center
    ws.row_dimensions[2].height = 16

    ws.append([])  # fila vacía

    # --- ENCABEZADOS ---
    columnas = {
        "fecha_operacion": "Fecha",
        "placa": "Placa",
        "conductor": "Conductor",
        "tipo_carga": "Tipo de Carga",
        "cantidad_texto": "Cantidad",
        "unidad_medida": "Unidad",
        "toneladas": "Toneladas",
        "descripcion": "Descripción",
    }

    col_keys = [k for k in columnas.keys() if k in df.columns]
    col_names = [columnas[k] for k in col_keys]

    header_row = 4
    for col_idx, name in enumerate(col_names, start=1):
        cell = ws.cell(row=header_row, column=col_idx, value=name)
        cell.font = font_header
        cell.fill = fill_header
        cell.alignment = align_center
        cell.border = border_thin
    ws.row_dimensions[header_row].height = 20

    # --- DATOS ---
    for row_idx, (_, row) in enumerate(df[col_keys].iterrows(), start=header_row + 1):
        fill_row = fill_alt if row_idx % 2 == 0 else None
        for col_idx, key in enumerate(col_keys, start=1):
            val = row[key]
            if pd.isna(val):
                val = ""
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.font = font_normal
            cell.border = border_thin
            cell.alignment = align_center if key in ("fecha_operacion", "placa", "cantidad_sacos", "toneladas") else align_left
            if fill_row:
                cell.fill = fill_row

    # --- FILA TOTALES ---
    total_row = header_row + len(df) + 1
    ws.cell(row=total_row, column=1, value="TOTALES").font = font_total
    ws.cell(row=total_row, column=1).fill = fill_total
    ws.cell(row=total_row, column=1).alignment = align_center

    # Total sacos
    if "cantidad_sacos" in col_keys:
        sacos_col = col_keys.index("cantidad_sacos") + 1
        sacos_letter = get_column_letter(sacos_col)
        cell_sacos = ws.cell(row=total_row, column=sacos_col)
        cell_sacos.value = f"=SUM({sacos_letter}{header_row+1}:{sacos_letter}{total_row-1})"
        cell_sacos.font = font_total
        cell_sacos.fill = fill_total
        cell_sacos.border = border_thin
        cell_sacos.alignment = align_center

    # Total toneladas
    if "toneladas" in col_keys:
        ton_col = col_keys.index("toneladas") + 1
        ton_letter = get_column_letter(ton_col)
        cell_ton = ws.cell(row=total_row, column=ton_col)
        cell_ton.value = f"=SUM({ton_letter}{header_row+1}:{ton_letter}{total_row-1})"
        cell_ton.font = font_total
        cell_ton.fill = fill_total
        cell_ton.border = border_thin
        cell_ton.alignment = align_center
        # Formato número colombiano para toda la columna toneladas
        for r in range(header_row + 1, total_row + 1):
            ws.cell(r, ton_col).number_format = '#,##0.00'

    # --- ANCHO DE COLUMNAS ---
    anchos = {
        "fecha_operacion": 14,
        "placa": 12,
        "conductor": 22,
        "tipo_carga": 22,
        "cantidad_texto": 16,
        "unidad_medida": 18,
        "toneladas": 14,
        "descripcion": 35,
    }
    for col_idx, key in enumerate(col_keys, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = anchos.get(key, 15)

    # --- HOJA RESUMEN ---
    ws2 = wb.create_sheet("Resumen")
    ws2["A1"] = "Resumen por Tipo de Carga"
    ws2["A1"].font = Font(name="Arial", bold=True, size=12, color="FFFFFF")
    ws2["A1"].fill = fill_titulo
    ws2.merge_cells("A1:D1")
    ws2["A1"].alignment = align_center

    ws2["A2"] = "Tipo de Carga"
    ws2["B2"] = "Viajes"
    ws2["C2"] = "Total Sacos"
    ws2["D2"] = "Total Toneladas"
    for col in ["A2", "B2", "C2", "D2"]:
        ws2[col].font = font_header
        ws2[col].fill = fill_header
        ws2[col].alignment = align_center
        ws2[col].border = border_thin

    if "tipo_carga" in df.columns:
        resumen = df.groupby("tipo_carga").agg(
            viajes=("id", "count"),
            sacos=("cantidad_sacos", "sum"),
            toneladas=("toneladas", "sum")
        ).reset_index()

        for r_idx, row in resumen.iterrows():
            r = r_idx + 3
            ws2.cell(r, 1, row["tipo_carga"]).border = border_thin
            ws2.cell(r, 2, int(row["viajes"])).border = border_thin
            ws2.cell(r, 3, int(row["sacos"])).border = border_thin
            ws2.cell(r, 4, round(float(row["toneladas"]), 2)).border = border_thin
            for c in range(1, 5):
                ws2.cell(r, c).font = font_normal
                ws2.cell(r, c).alignment = align_center

    for col_letter, width in zip(["A", "B", "C", "D"], [25, 10, 14, 16]):
        ws2.column_dimensions[col_letter].width = width

    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()


# ==================== MAIN ====================
def main():
    st.title("🚛 Operaciones Cartagena")

    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
    if 'editando_id' not in st.session_state:
        st.session_state.editando_id = None

    db = st.session_state.db

    tab0, tab1, tab2, tab3 = st.tabs(["📊 Dashboard Gerencial", "📝 Nuevo Registro", "🔍 Historial Detallado", "🚛 Gestión Vehículos"])

    # ============ TAB 0: DASHBOARD ============
    with tab0:
        st.markdown("### 📈 Resumen de Operaciones")

        col_filtro1, col_filtro2 = st.columns([2, 4])
        with col_filtro1:
            mes_actual = datetime.now()
            inicio_mes = mes_actual.replace(day=1)
            rango_fechas = st.date_input(
                "Rango de Análisis",
                value=(inicio_mes, mes_actual),
                key="dash_dates"
            )

        if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
            f_start, f_end = rango_fechas
            df_dash = db.obtener_datos_dashboard(f_start, f_end)

            if not df_dash.empty:
                total_ton = df_dash['toneladas'].sum()
                total_sacos = df_dash['cantidad_sacos'].sum()
                total_viajes = len(df_dash)

                k1, k2, k3 = st.columns(3)
                k1.metric("⚖️ Toneladas Movidas", f"{total_ton:,.2f}")
                k2.metric("📦 Sacos Movidos", f"{int(total_sacos):,}".replace(",", "."))
                k3.metric("🚚 Viajes Realizados", total_viajes)

                st.divider()

                c_chart1, c_chart2 = st.columns(2)

                with c_chart1:
                    st.subheader("🚛 Toneladas por Vehículo")
                    df_placa = df_dash.groupby("placa")['toneladas'].sum().reset_index().sort_values('toneladas', ascending=True)
                    fig_placa = px.bar(df_placa, x='toneladas', y='placa', orientation='h', text_auto='.2s', color='toneladas')
                    st.plotly_chart(fig_placa, use_container_width=True)

                with c_chart2:
                    st.subheader("📦 Toneladas por Tipo de Carga")
                    if 'tipo_carga' in df_dash.columns and df_dash['tipo_carga'].notna().any():
                        df_tipo = df_dash.groupby("tipo_carga")['toneladas'].sum().reset_index()
                        fig_tipo = px.pie(df_tipo, values='toneladas', names='tipo_carga', hole=0.4)
                        st.plotly_chart(fig_tipo, use_container_width=True)
                    else:
                        st.info("Sin datos de tipo de carga aún.")

                st.subheader("📆 Evolución Diaria de Toneladas")
                df_dia = df_dash.groupby("fecha_operacion")['toneladas'].sum().reset_index()
                fig_dia = px.line(df_dia, x='fecha_operacion', y='toneladas', markers=True)
                st.plotly_chart(fig_dia, use_container_width=True)

            else:
                st.info("No hay datos registrados en este rango de fechas.")
        else:
            st.info("Selecciona una fecha de inicio y fin para ver el reporte.")

    # ============ TAB 1: REGISTRO ============
    with tab1:
        st.markdown("### Registrar Movimiento")

        df_vehiculos = db.obtener_vehiculos_completo()
        lista_placas = []
        mapa_conductores = {}

        if not df_vehiculos.empty:
            lista_placas = df_vehiculos['placa'].tolist()
            mapa_conductores = {
                row['placa']: (row['conductor'] if row['conductor'] else "")
                for _, row in df_vehiculos.iterrows()
            }

        col1, col2 = st.columns(2)

        with col1:
            fecha_op = st.date_input("Fecha de Operación", datetime.now(), key="reg_fecha")
            placa_selec = st.selectbox("Placa / Unidad", lista_placas if lista_placas else [""], key="reg_placa")
            conductor_auto = mapa_conductores.get(placa_selec, "")
            conductor = st.text_input("Conductor Asignado", value=conductor_auto, key="reg_cond")

        with col2:
            tipo_carga = st.selectbox("Tipo de Carga", TIPOS_CARGA, key="reg_tipo")
            unidad = st.selectbox("Unidad de Medida", UNIDADES_MEDIDA, key="reg_unidad")
            cantidad_str = st.text_input(f"Cantidad ({unidad})", key="reg_cantidad", placeholder="Ej: 28.910,00")
            cantidad = parse_cantidad(cantidad_str) if cantidad_str.strip() else 0.0
            sacos = st.number_input("Cantidad de Sacos (opcional)", min_value=0, step=1, key="reg_sacos")

        descripcion = st.text_area("Descripción / Observaciones", key="reg_desc")

        st.markdown("#### 📸 Evidencia")
        archivo_foto = st.file_uploader("Subir foto", type=['png', 'jpg', 'jpeg'], key="reg_file")

        if st.button("💾 Guardar Registro", type="primary"):
            if not placa_selec or cantidad <= 0:
                st.error("⚠️ Faltan datos (Placa o Cantidad).")
            else:
                with st.spinner("Guardando..."):
                    img_bytes = None
                    fname = None
                    if archivo_foto:
                        img_bytes = procesar_imagen(archivo_foto)
                        fname = archivo_foto.name

                    if db.guardar_operacion(fecha_op, placa_selec, conductor, tipo_carga, unidad, descripcion, sacos, cantidad, cantidad_str, img_bytes, fname):
                        st.success(f"✅ Operación Guardada: {placa_selec} | {tipo_carga} | {cantidad_str} {unidad}")
                    else:
                        st.error("Error al guardar en base de datos.")

    # ============ TAB 2: HISTORIAL ============
    with tab2:
        st.markdown("### 🔍 Historial Detallado")

        with st.expander("🛠️ Filtros", expanded=True):
            c1, c2, c3, c4, c5 = st.columns(5)
            with c1: f_ini = st.date_input("Inicio", datetime.now() - timedelta(days=15), key="hist_ini")
            with c2: f_fin = st.date_input("Fin", datetime.now(), key="hist_fin")
            with c3:
                df_v = db.obtener_vehiculos_completo()
                l_placas = ["Todas"] + df_v['placa'].tolist() if not df_v.empty else ["Todas"]
                f_pla = st.selectbox("Filtrar Placa", l_placas, key="hist_placa")
            with c4:
                f_con = st.text_input("Buscar Conductor", key="hist_cond")
            with c5:
                f_tipo = st.selectbox("Tipo de Carga", ["Todos"] + TIPOS_CARGA, key="hist_tipo")

        df = db.obtener_historial(f_ini, f_fin, f_pla, f_con, f_tipo)

        if not df.empty:
            # --- BOTÓN DESCARGAR EXCEL ---
            st.markdown("#### 📥 Exportar")
            col_exp1, col_exp2 = st.columns([2, 6])
            with col_exp1:
                nombre_informe = st.text_input("Nombre del informe", value="Operaciones_Cartagena", key="excel_nombre")
            with col_exp2:
                st.markdown("<br>", unsafe_allow_html=True)
                excel_bytes = generar_excel(df, titulo=nombre_informe)
                st.download_button(
                    label="⬇️ Descargar Excel",
                    data=excel_bytes,
                    file_name=f"{nombre_informe}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )

            st.divider()

            # --- TABLA ---
            if 'tipo_carga' not in df.columns:
                df['tipo_carga'] = ''
            if 'unidad_medida' not in df.columns:
                df['unidad_medida'] = 'Toneladas (t)'
            if 'cantidad_texto' not in df.columns:
                df['cantidad_texto'] = df['toneladas'].astype(str)

            st.dataframe(
                df[['id', 'fecha_operacion', 'placa', 'conductor', 'tipo_carga', 'cantidad_texto', 'unidad_medida', 'toneladas', 'descripcion']],
                use_container_width=True,
                hide_index=True
            )

            st.divider()

            # --- VER / EDITAR / ELIMINAR ---
            st.subheader("✏️ Editar / Ver Detalle")
            df['label'] = df.apply(lambda x: f"ID {x['id']} | {x['fecha_operacion']} | {x['placa']} | {x.get('tipo_carga', '')} | {x['toneladas']} ton", axis=1)
            sel = st.selectbox("Seleccionar viaje:", df['label'].tolist(), key="hist_sel")

            if sel:
                id_s = int(sel.split(" | ")[0].replace("ID ", ""))
                row = df[df['id'] == id_s].iloc[0]

                col_izq, col_der = st.columns([1, 1])

                with col_izq:
                    # Foto
                    if row['nombre_archivo']:
                        imd = db.obtener_imagen(id_s)
                        if imd:
                            st.image(imd, caption=row['nombre_archivo'], use_container_width=True)
                    else:
                        st.info("Sin foto adjunta")

                with col_der:
                    # Modo edición
                    editando = st.session_state.editando_id == id_s

                    if not editando:
                        # Vista lectura
                        st.success(f"**Placa:** {row['placa']}")
                        st.info(f"**Conductor:** {row['conductor']}")
                        st.write(f"**Tipo de Carga:** {row.get('tipo_carga', 'N/A')}")
                        st.write(f"**Cantidad:** {row['cantidad_sacos']} sacos")
                        unidad_actual = row.get('unidad_medida', 'Toneladas (t)')
                        st.write(f"**Toneladas equiv.:** {row['toneladas']} t" if row['toneladas'] else f"**Cantidad:** {row['cantidad_sacos']} {unidad_actual}")
                        st.write(f"**Unidad:** {unidad_actual}")
                        st.write(f"**Descripción:** {row['descripcion']}")

                        btn_col1, btn_col2 = st.columns(2)
                        with btn_col1:
                            if st.button("✏️ Editar este viaje", key=f"edit_btn_{id_s}"):
                                st.session_state.editando_id = id_s
                                st.rerun()
                        with btn_col2:
                            if st.button("🗑️ Eliminar", key=f"del_{id_s}"):
                                db.eliminar_registro(id_s)
                                st.success("Registro eliminado.")
                                st.rerun()
                    else:
                        # Formulario de edición
                        st.markdown("#### ✏️ Editando viaje")

                        df_v2 = db.obtener_vehiculos_completo()
                        placas_edit = df_v2['placa'].tolist() if not df_v2.empty else [row['placa']]

                        fecha_edit = st.date_input("Fecha", value=row['fecha_operacion'], key=f"e_fecha_{id_s}")
                        placa_idx = placas_edit.index(row['placa']) if row['placa'] in placas_edit else 0
                        placa_edit = st.selectbox("Placa", placas_edit, index=placa_idx, key=f"e_placa_{id_s}")
                        cond_edit = st.text_input("Conductor", value=row['conductor'] or "", key=f"e_cond_{id_s}")

                        tipo_idx = TIPOS_CARGA.index(row['tipo_carga']) if row.get('tipo_carga') in TIPOS_CARGA else 0
                        tipo_edit = st.selectbox("Tipo de Carga", TIPOS_CARGA, index=tipo_idx, key=f"e_tipo_{id_s}")

                        unidad_actual = row.get('unidad_medida', 'Toneladas (t)')
                        unidad_idx = UNIDADES_MEDIDA.index(unidad_actual) if unidad_actual in UNIDADES_MEDIDA else 0
                        unidad_edit = st.selectbox("Unidad de Medida", UNIDADES_MEDIDA, index=unidad_idx, key=f"e_unidad_{id_s}")

                        # Mostrar cantidad según unidad actual almacenada
                        factor = CONVERSION_A_TONELADAS.get(unidad_actual)
                        val_cantidad = float(row['toneladas']) if (factor and row['toneladas']) else float(row['cantidad_sacos'] or 0)
                        key_cant = f"e_cant_{id_s}"
                        if key_cant not in st.session_state:
                            st.session_state[key_cant] = str(int(val_cantidad) if val_cantidad == int(val_cantidad) else val_cantidad)
                        cantidad_str_edit = st.text_input(f"Cantidad ({unidad_edit})", key=key_cant, placeholder="Ej: 28.910,00")
                        cantidad_edit = parse_cantidad(cantidad_str_edit) if cantidad_str_edit.strip() else val_cantidad
                        sacos_edit = st.number_input("Sacos (opcional)", min_value=0, value=int(row['cantidad_sacos'] or 0), key=f"e_sacos_{id_s}")
                        desc_edit = st.text_area("Descripción", value=row['descripcion'] or "", key=f"e_desc_{id_s}")

                        btn_g1, btn_g2 = st.columns(2)
                        with btn_g1:
                            if st.button("💾 Guardar Cambios", key=f"save_{id_s}", type="primary"):
                                if db.actualizar_operacion(id_s, fecha_edit, placa_edit, cond_edit, tipo_edit, unidad_edit, desc_edit, sacos_edit, cantidad_edit, cantidad_str_edit):
                                    st.success("✅ Registro actualizado.")
                                    st.session_state.editando_id = None
                                    st.rerun()
                        with btn_g2:
                            if st.button("❌ Cancelar", key=f"cancel_{id_s}"):
                                st.session_state.editando_id = None
                                st.rerun()
        else:
            st.warning("No hay datos con los filtros seleccionados.")

    # ============ TAB 3: VEHÍCULOS ============
    with tab3:
        st.subheader("🚛 Configuración de Flota")

        c1, c2 = st.columns([1, 2])

        with c1:
            with st.form("add_truck"):
                p_new = st.text_input("Placa").upper()
                p_con = st.text_input("Conductor Habitual")
                p_tip = st.selectbox("Tipo", ["Tractomula", "Dobletroque", "Sencillo", "Turbo"])

                if st.form_submit_button("Guardar / Actualizar"):
                    if p_new:
                        if db.guardar_vehiculo(p_new, p_tip, p_con):
                            st.success(f"✅ {p_new} Guardada")
                            st.rerun()

        with c2:
            df_v = db.obtener_vehiculos_completo()
            if not df_v.empty:
                st.dataframe(df_v, use_container_width=True, hide_index=True)
                p_del = st.selectbox("Seleccionar para eliminar:", df_v['placa'].tolist(), key="veh_del")
                if st.button("🗑️ Eliminar Vehículo"):
                    db.eliminar_vehiculo(p_del)
                    st.rerun()


if __name__ == "__main__":
    main()
