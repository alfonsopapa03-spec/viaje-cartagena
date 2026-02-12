import streamlit as st
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from PIL import Image
import io
import plotly.express as px  # NUEVA LIBRERÍA PARA GRÁFICOS

# ==================== CONFIGURACIÓN DE PÁGINA ====================
st.set_page_config(
    page_title="Logística Cartagena", 
    layout="wide", 
    page_icon="🚛",
    initial_sidebar_state="collapsed"
)

# ==================== CREDENCIALES SUPABASE ====================
SUPABASE_DB_URL = "postgresql://postgres.verwlkgitpllyneqxlao:Conejito800$@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

# ==================== GESTOR DE BASE DE DATOS ====================
class DatabaseManager:
    def __init__(self):
        self.db_url = SUPABASE_DB_URL
        self.init_database()

    def get_connection(self):
        return psycopg2.connect(self.db_url)

    def init_database(self):
        """Inicializa tablas y actualiza columnas si faltan"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # 1. Tabla Vehículos
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

            # 2. Tabla Operaciones
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS operaciones_cartagena (
                    id SERIAL PRIMARY KEY,
                    fecha_registro TIMESTAMP DEFAULT (now() AT TIME ZONE 'America/Bogota'),
                    fecha_operacion DATE NOT NULL,
                    placa TEXT NOT NULL,
                    conductor TEXT,
                    descripcion TEXT,
                    cantidad_sacos INTEGER,
                    toneladas REAL,
                    imagen_comprobante BYTEA,
                    nombre_archivo TEXT
                )
            ''')
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_op_fecha ON operaciones_cartagena(fecha_operacion);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_op_placa ON operaciones_cartagena(placa);")

            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Error DB: {e}")

    # --- DATOS GENERALES PARA DASHBOARD ---
    def obtener_datos_dashboard(self, fecha_inicio, fecha_fin):
        conn = self.get_connection()
        query = """
            SELECT fecha_operacion, placa, conductor, cantidad_sacos, toneladas 
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

    # --- VEHÍCULOS ---
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

    # --- OPERACIONES ---
    def guardar_operacion(self, fecha, placa, conductor, descripcion, sacos, toneladas, imagen_bytes, nombre_archivo):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = '''
                INSERT INTO operaciones_cartagena 
                (fecha_operacion, placa, conductor, descripcion, cantidad_sacos, toneladas, imagen_comprobante, nombre_archivo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            '''
            if imagen_bytes:
                imagen_bytes = psycopg2.Binary(imagen_bytes)
                
            cursor.execute(sql, (fecha, placa, conductor, descripcion, sacos, toneladas, imagen_bytes, nombre_archivo))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Error guardando: {e}")
            return False

    def obtener_historial(self, fecha_inicio=None, fecha_fin=None, placa=None, conductor=None):
        conn = self.get_connection()
        query = """
            SELECT id, fecha_operacion, placa, conductor, descripcion, 
                   cantidad_sacos, toneladas, nombre_archivo 
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
def procesar_imagen(uploaded_file):
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

# ==================== MAIN ====================
def main():
    st.title("🚛 Operaciones Cartagena")
    
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
    
    db = st.session_state.db

    # DEFINICIÓN DE PESTAÑAS (Ahora el Dashboard es la primera)
    tab0, tab1, tab2, tab3 = st.tabs(["📊 Dashboard Gerencial", "📝 Nuevo Registro", "🔍 Historial Detallado", "🚛 Gestión Vehículos"])

    # ---------------- TAB 0: DASHBOARD ----------------
    with tab0:
        st.markdown("### 📈 Resumen de Operaciones")
        
        # Filtro de fecha para el dashboard
        col_filtro1, col_filtro2 = st.columns([1, 4])
        with col_filtro1:
            mes_actual = datetime.now()
            inicio_mes = mes_actual.replace(day=1)
            rango_fechas = st.date_input(
                "Rango de Análisis",
                value=(inicio_mes, mes_actual),
                key="dash_dates"
            )
        
        # Validar que sea un rango
        if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
            f_start, f_end = rango_fechas
            df_dash = db.obtener_datos_dashboard(f_start, f_end)
            
            if not df_dash.empty:
                # --- KPI CARDS ---
                total_ton = df_dash['toneladas'].sum()
                total_sacos = df_dash['cantidad_sacos'].sum()
                total_viajes = len(df_dash)
                
                k1, k2, k3 = st.columns(3)
                k1.metric("⚖️ Toneladas Movidas", f"{total_ton:,.2f}", delta="Total Periodo")
                k2.metric("📦 Sacos Movidos", f"{int(total_sacos):,}".replace(",", "."), delta="Total Periodo")
                k3.metric("🚚 Viajes Realizados", total_viajes, delta="Despachos")
                
                st.divider()
                
                # --- GRÁFICOS ---
                c_chart1, c_chart2 = st.columns(2)
                
                with c_chart1:
                    st.subheader("🚛 Toneladas por Vehículo")
                    # Agrupar por placa
                    df_placa = df_dash.groupby("placa")['toneladas'].sum().reset_index().sort_values('toneladas', ascending=True)
                    fig_placa = px.bar(df_placa, x='toneladas', y='placa', orientation='h', text_auto='.2s', color='toneladas')
                    st.plotly_chart(fig_placa, use_container_width=True)

                with c_chart2:
                    st.subheader("📆 Evolución Diaria")
                    # Agrupar por fecha
                    df_dia = df_dash.groupby("fecha_operacion")['toneladas'].sum().reset_index()
                    fig_dia = px.line(df_dia, x='fecha_operacion', y='toneladas', markers=True, title="Toneladas por Día")
                    st.plotly_chart(fig_dia, use_container_width=True)
                
                # --- PIE CHART CONDUCTORES ---
                st.subheader("👤 Participación por Conductor")
                df_cond = df_dash.groupby("conductor")['toneladas'].sum().reset_index()
                fig_cond = px.pie(df_cond, values='toneladas', names='conductor', hole=0.4)
                st.plotly_chart(fig_cond, use_container_width=True)

            else:
                st.info("No hay datos registrados en este rango de fechas.")
        else:
            st.info("Selecciona una fecha de inicio y fin para ver el reporte.")

    # ---------------- TAB 1: REGISTRO ----------------
    with tab1:
        st.markdown("### Registrar Movimiento")
        
        df_vehiculos = db.obtener_vehiculos_completo()
        
        lista_placas = []
        mapa_conductores = {}
        
        if not df_vehiculos.empty:
            lista_placas = df_vehiculos['placa'].tolist()
            mapa_conductores = {
                row['placa']: (row['conductor'] if row['conductor'] else "")
                for index, row in df_vehiculos.iterrows()
            }

        col1, col2 = st.columns(2)
        
        with col1:
            fecha_op = st.date_input("Fecha de Operación", datetime.now(), key="reg_fecha")
            placa_selec = st.selectbox("Placa / Unidad", lista_placas if lista_placas else [""], key="reg_placa")
            
            # Autocompletado manual en el input
            conductor_auto = mapa_conductores.get(placa_selec, "")
            
            # Usamos key para manejar el estado si es necesario, pero el value lo llena
            conductor = st.text_input("Conductor Asignado", value=conductor_auto, key="reg_cond")

        with col2:
            sacos = st.number_input("Cantidad de Sacos", min_value=0, step=1, key="reg_sacos")
            toneladas = st.number_input("Total Toneladas", min_value=0.0, step=0.1, format="%.2f", key="reg_ton")
            
        descripcion = st.text_area("Descripción / Observaciones", key="reg_desc")
        
        st.markdown("#### 📸 Evidencia")
        archivo_foto = st.file_uploader("Subir foto", type=['png', 'jpg', 'jpeg'], key="reg_file")
        
        if st.button("💾 Guardar Registro", type="primary"):
            if not placa_selec or sacos <= 0 or toneladas <= 0:
                st.error("⚠️ Faltan datos (Placa, Sacos o Toneladas).")
            else:
                with st.spinner("Guardando..."):
                    img_bytes = None
                    fname = None
                    if archivo_foto:
                        img_bytes = procesar_imagen(archivo_foto)
                        fname = archivo_foto.name
                    
                    if db.guardar_operacion(fecha_op, placa_selec, conductor, descripcion, sacos, toneladas, img_bytes, fname):
                        st.success(f"✅ Operación Guardada: {placa_selec} ({toneladas} ton)")
                    else:
                        st.error("Error al guardar en base de datos.")

    # ---------------- TAB 2: HISTORIAL ----------------
    with tab2:
        st.markdown("### 🔍 Historial Detallado")
        with st.expander("🛠️ Filtros", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            with c1: f_ini = st.date_input("Inicio", datetime.now() - timedelta(days=15), key="hist_ini")
            with c2: f_fin = st.date_input("Fin", datetime.now(), key="hist_fin")
            with c3: 
                df_v = db.obtener_vehiculos_completo()
                l_placas = ["Todas"] + df_v['placa'].tolist() if not df_v.empty else ["Todas"]
                f_pla = st.selectbox("Filtrar Placa", l_placas, key="hist_placa")
            with c4: f_con = st.text_input("Buscar Conductor", key="hist_cond")

        df = db.obtener_historial(f_ini, f_fin, f_pla, f_con)
        
        if not df.empty:
            st.dataframe(df[['fecha_operacion', 'placa', 'conductor', 'descripcion', 'cantidad_sacos', 'toneladas']], use_container_width=True, hide_index=True)
            
            st.subheader("🖼️ Ver Foto y Eliminar")
            df['ver'] = df.apply(lambda x: f"ID {x['id']} | {x['fecha_operacion']} | {x['placa']}", axis=1)
            sel = st.selectbox("Seleccionar viaje:", df['ver'].tolist(), key="hist_sel")
            
            if sel:
                id_s = int(sel.split(" | ")[0].replace("ID ", ""))
                row = df[df['id'] == id_s].iloc[0]
                c_img, c_dat = st.columns([1,1])
                with c_img:
                    if row['nombre_archivo']:
                        imd = db.obtener_imagen(id_s)
                        if imd: st.image(imd, caption=row['nombre_archivo'], use_container_width=True)
                    else: st.info("Sin foto")
                with c_dat:
                    st.success(f"Placa: {row['placa']}")
                    st.info(f"Conductor: {row['conductor']}")
                    st.write(f"Sacos: {row['cantidad_sacos']}")
                    st.write(f"Notas: {row['descripcion']}")
                    if st.button("🗑️ Eliminar este registro", key=f"del_{id_s}"):
                        db.eliminar_registro(id_s)
                        st.rerun()
        else:
            st.warning("No hay datos.")

    # ---------------- TAB 3: VEHÍCULOS ----------------
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
