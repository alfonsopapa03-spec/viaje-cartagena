import streamlit as st
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from PIL import Image
import io

# ==================== CONFIGURACIÓN DE PÁGINA ====================
st.set_page_config(
    page_title="Logística Cartagena", 
    layout="wide", 
    page_icon="🚛",
    initial_sidebar_state="collapsed"
)

# ==================== CREDENCIALES SUPABASE ====================
SUPABASE_DB_URL = "postgresql://postgres.scjqqcrkjdavetdyxtrf:GV69W?B8v$x4wH?@aws-1-us-east-1.pooler.supabase.com:6543/postgres"


# ==================== GESTOR DE BASE DE DATOS ====================
class DatabaseManager:
    def __init__(self):
        self.db_url = SUPABASE_DB_URL
        self.init_database()

    def get_connection(self):
        return psycopg2.connect(self.db_url)

    def init_database(self):
        """Crea las tablas y actualiza la estructura si es necesario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # 1. Tabla de Vehículos (Ahora con columna conductor)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tractomulas (
                    id SERIAL PRIMARY KEY,
                    placa TEXT UNIQUE NOT NULL,
                    tipo TEXT,
                    conductor TEXT
                )
            ''')
            
            # --- MIGRACIÓN AUTOMÁTICA ---
            # Intentamos agregar la columna 'conductor' si la tabla ya existía sin ella
            try:
                cursor.execute("ALTER TABLE tractomulas ADD COLUMN IF NOT EXISTS conductor TEXT")
                conn.commit()
            except Exception:
                conn.rollback() # Si falla o ya existe, seguimos

            # 2. Tabla de Operaciones
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

            # Indices
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_op_fecha ON operaciones_cartagena(fecha_operacion);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_op_placa ON operaciones_cartagena(placa);")

            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Error inicializando base de datos: {e}")

    # --- VEHÍCULOS ---
    def obtener_vehiculos_completo(self):
        """Devuelve un DataFrame con placa y conductor predeterminado"""
        conn = self.get_connection()
        try:
            # Traemos placa y conductor
            df = pd.read_sql("SELECT placa, conductor FROM tractomulas ORDER BY placa", conn)
            return df
        except:
            return pd.DataFrame(columns=['placa', 'conductor'])
        finally:
            conn.close()

    def guardar_vehiculo(self, placa, tipo, conductor):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            # Usamos ON CONFLICT para ACTUALIZAR el conductor si la placa ya existe
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
        except Exception as e:
            st.error(f"Error DB: {e}")
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
            st.error(f"Error guardando operación: {e}")
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
        st.error(f"Error img: {e}")
        return None

# ==================== MAIN ====================
def main():
    st.title("🚛 Operaciones Cartagena - Día a Día")
    
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
    
    db = st.session_state.db

    tab1, tab2, tab3 = st.tabs(["📝 Nuevo Registro", "🔍 Historial y Trazabilidad", "🚛 Gestión Vehículos"])

    # ---------------- TAB 1: REGISTRO ----------------
    with tab1:
        st.markdown("### Registrar Movimiento")
        
        with st.form("form_operacion", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                fecha_op = st.date_input("Fecha de Operación", datetime.now())
                
                # Obtener vehículos y crear mapa de conductores
                df_vehiculos = db.obtener_vehiculos_completo()
                
                if df_vehiculos.empty:
                    st.warning("⚠️ Registra vehículos en la pestaña 3.")
                    placas_list = []
                    mapa_conductores = {}
                else:
                    placas_list = df_vehiculos['placa'].tolist()
                    # Creamos un diccionario: {'ABC-123': 'Juan', 'XYZ-999': 'Pedro'}
                    mapa_conductores = dict(zip(df_vehiculos['placa'], df_vehiculos['conductor']))

                # Selector de placa
                placa_selec = st.selectbox("Placa / Unidad", placas_list if placas_list else [""])
                
                # Autocompletar conductor basado en la placa seleccionada
                conductor_defecto = ""
                if placa_selec in mapa_conductores and mapa_conductores[placa_selec]:
                    conductor_defecto = mapa_conductores[placa_selec]

                conductor = st.text_input("Conductor Asignado", value=conductor_defecto)

            with col2:
                sacos = st.number_input("Cantidad de Sacos", min_value=0, step=1)
                toneladas = st.number_input("Total Toneladas", min_value=0.0, step=0.1, format="%.2f")
                
            descripcion = st.text_area("Descripción / Observaciones")
            
            st.markdown("#### 📸 Evidencia")
            archivo_foto = st.file_uploader("Subir foto", type=['png', 'jpg', 'jpeg'])
            
            if st.form_submit_button("💾 Guardar Registro", type="primary"):
                if not placa_selec or sacos <= 0 or toneladas <= 0:
                    st.error("⚠️ Faltan datos (Placa, Sacos o Toneladas).")
                else:
                    img_bytes = None
                    fname = None
                    if archivo_foto:
                        img_bytes = procesar_imagen(archivo_foto)
                        fname = archivo_foto.name
                    
                    if db.guardar_operacion(fecha_op, placa_selec, conductor, descripcion, sacos, toneladas, img_bytes, fname):
                        st.success(f"✅ Guardado: {placa_selec} - {conductor}")
                    else:
                        st.error("Error al guardar.")

    # ---------------- TAB 2: HISTORIAL ----------------
    with tab2:
        st.markdown("### 🔍 Trazabilidad")
        with st.expander("🛠️ Filtros", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            with c1: f_ini = st.date_input("Inicio", datetime.now() - timedelta(days=15))
            with c2: f_fin = st.date_input("Fin", datetime.now())
            with c3: 
                df_v = db.obtener_vehiculos_completo()
                lista_p = ["Todas"] + df_v['placa'].tolist() if not df_v.empty else ["Todas"]
                f_pla = st.selectbox("Placa", lista_p)
            with c4: f_con = st.text_input("Buscar Conductor")

        df = db.obtener_historial(f_ini, f_fin, f_pla, f_con)
        
        if not df.empty:
            m1, m2, m3 = st.columns(3)
            m1.metric("📦 Sacos", f"{int(df['cantidad_sacos'].sum()):,}".replace(",", "."))
            m2.metric("⚖️ Toneladas", f"{df['toneladas'].sum():,.2f}")
            m3.metric("🚚 Viajes", len(df))
            
            st.dataframe(df[['fecha_operacion', 'placa', 'conductor', 'descripcion', 'cantidad_sacos', 'toneladas']], use_container_width=True, hide_index=True)
            
            st.subheader("🖼️ Ver Foto")
            df['ver'] = df.apply(lambda x: f"ID {x['id']} | {x['fecha_operacion']} | {x['placa']}", axis=1)
            sel = st.selectbox("Seleccionar viaje:", df['ver'].tolist())
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
                    if st.button("🗑️ Eliminar", key=f"d{id_s}"):
                        db.eliminar_registro(id_s)
                        st.rerun()
        else:
            st.warning("No hay datos.")

    # ---------------- TAB 3: VEHÍCULOS ----------------
    with tab3:
        st.subheader("🚛 Configuración de Flota")
        st.info("Asigna un conductor a cada placa para que se cargue automáticamente.")
        
        c1, c2 = st.columns([1, 2])
        
        with c1:
            with st.form("add_truck"):
                p_new = st.text_input("Placa").upper()
                p_con = st.text_input("Conductor Habitual")
                p_tip = st.selectbox("Tipo", ["Tractomula", "Dobletroque", "Sencillo", "Turbo"])
                
                if st.form_submit_button("Guardar / Actualizar"):
                    if p_new:
                        if db.guardar_vehiculo(p_new, p_tip, p_con):
                            st.success(f"✅ {p_new} asignada a {p_con}")
                            st.rerun()
        
        with c2:
            st.markdown("#### Lista de Unidades")
            df_v = db.obtener_vehiculos_completo()
            if not df_v.empty:
                st.dataframe(df_v, use_container_width=True, hide_index=True)
                
                # Borrado simple por input
                p_del = st.selectbox("Seleccionar para eliminar:", df_v['placa'].tolist())
                if st.button("🗑️ Eliminar Vehículo"):
                    db.eliminar_vehiculo(p_del)
                    st.rerun()

if __name__ == "__main__":
    main()
