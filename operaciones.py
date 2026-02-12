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
        """Crea las tablas necesarias automáticamente"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # 1. Tabla de Vehículos
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tractomulas (
                    id SERIAL PRIMARY KEY,
                    placa TEXT UNIQUE NOT NULL,
                    tipo TEXT
                )
            ''')

            # 2. Tabla de Operaciones (Con soporte para fotos BYTEA)
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

            # Indices para búsqueda rápida
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_op_fecha ON operaciones_cartagena(fecha_operacion);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_op_placa ON operaciones_cartagena(placa);")

            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Error inicializando base de datos: {e}")

    # --- VEHÍCULOS ---
    def obtener_placas(self):
        conn = self.get_connection()
        try:
            df = pd.read_sql("SELECT placa FROM tractomulas ORDER BY placa", conn)
            return df['placa'].tolist()
        except:
            return []
        finally:
            conn.close()

    def guardar_vehiculo(self, placa, tipo):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO tractomulas (placa, tipo) VALUES (%s, %s) ON CONFLICT DO NOTHING", (placa, tipo))
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
            # psycopg2 maneja bytes automáticamente, pero aseguramos que sea bytes
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
        
        # Filtro Fecha
        if fecha_inicio:
            query += " AND fecha_operacion >= %s"
            params.append(fecha_inicio)
        if fecha_fin:
            query += " AND fecha_operacion <= %s"
            params.append(fecha_fin)
            
        # Filtro Placa
        if placa and placa != "Todas":
            query += " AND placa = %s"
            params.append(placa)
            
        # Filtro Conductor (ILIKE busca ignorando mayúsculas/minúsculas)
        if conductor:
            query += " AND conductor ILIKE %s"
            params.append(f"%{conductor}%")
            
        query += " ORDER BY fecha_operacion DESC, id DESC"
        
        try:
            df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            st.error(f"Error buscando historial: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def obtener_imagen(self, registro_id):
        """Recupera la imagen y la convierte a bytes para evitar error memoryview"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT imagen_comprobante FROM operaciones_cartagena WHERE id = %s", (registro_id,))
        result = cursor.fetchone()
        conn.close()
        
        # CORRECCIÓN CRÍTICA: Convertir memoryview a bytes explícitamente
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

# ==================== UTILIDADES DE IMAGEN ====================
def procesar_imagen(uploaded_file):
    """Redimensiona y comprime la imagen para optimizar la BD"""
    if uploaded_file is None:
        return None
    
    try:
        image = Image.open(uploaded_file)
        # Convertir a RGB si es PNG transparente
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
            
        # Redimensionar (Max ancho 1024px)
        max_width = 1024
        if image.width > max_width:
            ratio = max_width / image.width
            new_height = int(image.height * ratio)
            image = image.resize((max_width, new_height), Image.Resampling.LANCZOS)
        
        # Guardar en buffer como JPEG comprimido
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='JPEG', quality=70)
        return img_byte_arr.getvalue()
    except Exception as e:
        st.error(f"Error procesando imagen: {e}")
        return None

# ==================== INTERFAZ PRINCIPAL ====================
def main():
    st.title("🚛 Operaciones Cartagena - Día a Día")
    
    # Inicializar DB en Session State
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
    
    db = st.session_state.db

    # Estilo de pestañas
    tab1, tab2, tab3 = st.tabs(["📝 Nuevo Registro", "🔍 Historial y Trazabilidad", "🚛 Gestión Vehículos"])

    # ---------------- TAB 1: REGISTRO ----------------
    with tab1:
        st.markdown("### Registrar Movimiento Diario")
        st.info("Ingresa los datos del viaje y sube la foto del comprobante.")
        
        with st.form("form_operacion", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                fecha_op = st.date_input("Fecha de Operación", datetime.now())
                
                # Cargar placas disponibles
                placas_disponibles = db.obtener_placas()
                if not placas_disponibles:
                    st.warning("⚠️ No hay vehículos registrados. Ve a la pestaña 3.")
                    placa_selec = st.text_input("Placa (Manual)")
                else:
                    placa_selec = st.selectbox("Placa / Unidad", placas_disponibles)
                
                conductor = st.text_input("Nombre del Conductor")

            with col2:
                sacos = st.number_input("Cantidad de Sacos", min_value=0, step=1)
                toneladas = st.number_input("Total Toneladas", min_value=0.0, step=0.1, format="%.2f")
                
            descripcion = st.text_area("Descripción / Observaciones (Origen, Destino, Cliente)")
            
            st.markdown("#### 📸 Evidencia")
            archivo_foto = st.file_uploader("Subir foto del comprobante", type=['png', 'jpg', 'jpeg'])
            
            btn_guardar = st.form_submit_button("💾 Guardar Registro", type="primary")
            
            if btn_guardar:
                if not placa_selec or sacos <= 0 or toneladas <= 0:
                    st.error("⚠️ Error: Debes ingresar Placa, Sacos y Toneladas.")
                else:
                    with st.spinner("Procesando imagen y guardando..."):
                        # Procesar imagen
                        imagen_bytes = None
                        nombre_archivo = None
                        if archivo_foto:
                            imagen_bytes = procesar_imagen(archivo_foto)
                            nombre_archivo = archivo_foto.name
                        
                        exito = db.guardar_operacion(
                            fecha_op, placa_selec, conductor, descripcion, 
                            sacos, toneladas, imagen_bytes, nombre_archivo
                        )
                        
                        if exito:
                            st.success(f"✅ Registro guardado para {placa_selec} ({toneladas} ton).")
                        else:
                            st.error("❌ Error al guardar en la base de datos.")

    # ---------------- TAB 2: HISTORIAL ----------------
    with tab2:
        st.markdown("### 🔍 Trazabilidad de Viajes")
        
        # Filtros
        with st.expander("🛠️ Filtros de Búsqueda", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                f_inicio = st.date_input("Fecha Inicio", datetime.now() - timedelta(days=15))
            with c2:
                f_fin = st.date_input("Fecha Fin", datetime.now())
            with c3:
                lista_placas = ["Todas"] + db.obtener_placas()
                f_placa = st.selectbox("Filtrar Placa", lista_placas)
            with c4:
                f_conductor = st.text_input("Buscar Conductor")

        # Obtener datos
        df = db.obtener_historial(f_inicio, f_fin, f_placa, f_conductor)
        
        if df.empty:
            st.warning("No se encontraron viajes con estos filtros.")
        else:
            # Métricas Totales
            total_sacos = df['cantidad_sacos'].sum()
            total_ton = df['toneladas'].sum()
            
            st.markdown("---")
            m1, m2, m3 = st.columns(3)
            m1.metric("📦 Total Sacos", f"{int(total_sacos):,}".replace(",", "."))
            m2.metric("⚖️ Total Toneladas", f"{total_ton:,.2f}")
            m3.metric("🚚 Cantidad Viajes", len(df))
            st.markdown("---")

            # Tabla de Datos
            st.dataframe(
                df[['fecha_operacion', 'placa', 'conductor', 'descripcion', 'cantidad_sacos', 'toneladas']], 
                use_container_width=True,
                hide_index=True
            )
            
            # Visor de Detalles e Imagen
            st.subheader("🖼️ Ver Comprobante y Detalles")
            
            # Selector inteligente
            # Creamos una columna combinada para el selectbox
            df['display'] = df.apply(lambda x: f"ID {x['id']} | {x['fecha_operacion']} | {x['placa']} | {x['conductor']}", axis=1)
            opciones = df['display'].tolist()
            
            seleccion = st.selectbox("Selecciona un viaje de la lista:", opciones)
            
            if seleccion:
                id_sel = int(seleccion.split(" | ")[0].replace("ID ", ""))
                fila = df[df['id'] == id_sel].iloc[0]
                
                col_img, col_det = st.columns([1, 1])
                
                with col_img:
                    st.caption("📸 Comprobante:")
                    if fila['nombre_archivo']:
                        img_data = db.obtener_imagen(id_sel)
                        if img_data:
                            # CORRECCIÓN DE VISUALIZACIÓN
                            st.image(img_data, caption=fila['nombre_archivo'], use_container_width=True)
                        else:
                            st.error("Error cargando la imagen.")
                    else:
                        st.info("Este registro no tiene foto adjunta.")
                        
                with col_det:
                    st.markdown(f"### Detalles del Viaje ID: {id_sel}")
                    st.success(f"**Placa:** {fila['placa']}")
                    st.info(f"**Conductor:** {fila['conductor']}")
                    st.write(f"**Fecha:** {fila['fecha_operacion']}")
                    st.write(f"**Sacos:** {fila['cantidad_sacos']}")
                    st.write(f"**Toneladas:** {fila['toneladas']}")
                    st.markdown(f"**Observaciones:**\n{fila['descripcion']}")
                    
                    st.markdown("---")
                    if st.button("🗑️ Eliminar este registro", key=f"del_{id_sel}"):
                        if db.eliminar_registro(id_sel):
                            st.success("Registro eliminado.")
                            st.rerun()

    # ---------------- TAB 3: VEHÍCULOS ----------------
    with tab3:
        st.subheader("🚛 Gestión de Flota")
        
        c1, c2 = st.columns([1, 2])
        
        with c1:
            st.markdown("#### Agregar Unidad")
            with st.form("add_truck"):
                p_nueva = st.text_input("Placa").upper()
                p_tipo = st.selectbox("Tipo", ["Tractomula", "Dobletroque", "Sencillo", "Turbo"])
                if st.form_submit_button("Guardar"):
                    if p_nueva:
                        db.guardar_vehiculo(p_nueva, p_tipo)
                        st.success("Guardado.")
                        st.rerun()
        
        with c2:
            st.markdown("#### Unidades Registradas")
            placas = db.obtener_placas()
            if placas:
                for p in placas:
                    col_a, col_b = st.columns([4, 1])
                    col_a.text(f"🚛 {p}")
                    if col_b.button("❌", key=f"borrar_placa_{p}"):
                        db.eliminar_vehiculo(p)
                        st.rerun()
            else:
                st.info("No hay vehículos registrados.")

if __name__ == "__main__":
    main()