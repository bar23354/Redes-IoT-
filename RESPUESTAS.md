# Respuestas a las Preguntas del Laboratorio: Estación Meteorológica IoT

Universidad del Valle de Guatemala  
CC3067 - Redes  
Integrantes:  
- Roberto Barreda, 23354  
- Javier España, 23361  

---

## Sección 3.1: Simulación de un Sensor

### 1. ¿A qué capa pertenece JSON/SOAP según el Modelo OSI y por qué?

**Respuesta:**  
Pertenecen a la **Capa de Presentación (Capa 6)** del Modelo OSI.

**Justificación:**  
La función principal de la Capa de Presentación es la sintaxis, estructuración, formateo, serialización y traducción de datos para que aplicaciones en sistemas heterogéneos puedan entenderlos sin importar sus diferencias internas de arquitectura de procesador (endianness, alineación de memoria, etc.). 
- Tanto **JSON** (*JavaScript Object Notation*) como **SOAP** (*Simple Object Access Protocol*, basado en XML) actúan como capas de serialización/representación de datos antes de ser consumidos por la lógica de negocio en la Capa de Aplicación (Capa 7).
- Nota práctica: En el modelo TCP/IP, las capas 5 (Sesión), 6 (Presentación) y 7 (Aplicación) se unifican en la única **Capa de Aplicación**, por lo que en dicho modelo se ubican comúnmente allí.

---

### 2. ¿Qué beneficios tiene utilizar un formato como JSON/SOAP?

**Respuesta:**
1. **Interoperabilidad e Independencia de Plataforma:** Permiten que sistemas escritos en diferentes lenguajes (C en microcontroladores, Python en brokers/servicios Edge, TypeScript en frontends) intercambien información sin problemas de incompatibilidad de tipos primitivos.
2. **Estructura y Auto-descriptividad:** Los datos vienen acompañados de claves semánticas (`"temperatura": 56.32`), lo que facilita su lectura por humanos, inspección con herramientas de red y depuración.
3. **Evolución y Flexibilidad del Esquema:** Es sencillo agregar nuevos campos (por ejemplo, `"presion": 1013.25`) sin romper inmediatamente a los consumidores existentes que ignoren campos desconocidos.
4. **Validación y Ecosistema de Herramientas:** Existen librerías estándar en prácticamente cualquier lenguaje para serializar, deserializar y validar esquemas (JSON Schema o XSD en SOAP).

---

## Sección 3.3: Consumir y Desplegar Datos Meteorológicos

### 3. ¿Qué ventajas y desventajas considera que tiene este acercamiento basado en Pub/Sub de Kafka?

**Ventajas:**
- **Desacoplamiento temporal y espacial:** Los productores (sensores IoT) y los consumidores (paneles de telemetría, bases de datos históricas, alertas) no se conocen directamente ni necesitan estar activos al mismo tiempo.
- **Escalabilidad horizontal masiva:** Mediante particiones de topics y grupos de consumidores (*Consumer Groups*), múltiples instancias de consumidores pueden procesar eventos en paralelo.
- **Persistencia y Reprocesamiento (*Time-Travel*):** A diferencia de colas tradicionales donde los mensajes se eliminan al consumirse, Kafka retiene los mensajes en disco según una política de retención. Un consumidor nuevo o caído puede leer desde el inicio (`auto.offset.reset=earliest`) para reconstruir el estado completo.
- **Tolerancia a fallos y alta disponibilidad:** El cluster de Kafka replica particiones entre múltiples brokers.

**Desventajas:**
- **Sobrecarga (Overhead) de infraestructura y recursos:** Kafka requiere brokers dedicados, JVM, gestión de metadatos (KRaft o ZooKeeper) y almacenamiento en disco, lo cual es excesivo para proyectos IoT pequeños.
- **No apto para dispositivos IoT de muy bajos recursos:** Los microcontroladores (ej. ESP32 o Arduino) rara vez pueden ejecutar un cliente nativo completo de Kafka (protocolo binario TCP complejo, SASL/TLS, buffers). En entornos IoT reales se suele usar un Gateway o Edge Server intermedio (MQTT / HTTP a Kafka).
- **Latencia frente a sockets puros:** Aunque Kafka es de alto rendimiento, introduce latencia por buffers y escritura en disco en comparación con protocolos UDP directos o sockets en memoria.

---

### 4. ¿Para qué aplicaciones tiene sentido usar Kafka? ¿Para cuáles no?

**Tiene sentido en:**
- **Streaming de telemetría a gran escala:** Flotas vehiculares, redes eléctricas inteligentes, miles de sensores industriales enviando métricas por segundo.
- **Pipelines de analítica en tiempo real (Big Data):** Ingesta hacia motores como Apache Flink, Spark Streaming, ClickHouse o Elasticsearch.
- **Arquitecturas dirigidas por eventos (EDA) y Event Sourcing:** Registro inmutable de transacciones financieras, pedidos de e-commerce y logs de auditoría.
- **Métricas y observabilidad centralizada:** Agregación de logs distribuidos de microservicios.

**NO tiene sentido en:**
- **Microcontroladores sin Edge Gateway:** Dispositivos ultra-restringidos con batería y micro-redes (LoRaWAN, Sigfox, BLE) donde cada byte y cada ciclo de CPU cuenta.
- **Sistemas de control en tiempo real estricto (*Hard Real-Time*):** Robótica industrial, aviónica o frenos automotrices donde los plazos de respuesta son de microsegundos deterministas.
- **Mensajería punto a punto simple o baja escala:** Un CRUD web tradicional o un sistema con pocos mensajes por día; en esos casos RabbitMQ, Redis Pub/Sub o MQTT son mucho más simples y livianos.
- **Notificaciones interactivas tipo Chat 1-a-1 de baja concurrencia:** No requiere el almacenamiento persistente ni la partición de Kafka.

---

## Sección 3.4: IoT en Entornos con Restricciones

### 5. ¿Qué complejidades introduce el tener un payload restringido (pequeño)?

**Respuesta:**
1. **Pérdida de Auto-descriptividad (Esquema implícito):** Se pierden las etiquetas de texto (como `"temperatura"`). Tanto el emisor como el receptor deben compartir estrictamente el mismo contrato a nivel de bits (*bit layout*, offsets, endianness). Si uno cambia, los datos se corrompen.
2. **Necesidad de Cuantización y Pérdida de Rango o Precisión:** Se debe delimitar exactamente el rango mínimo, máximo y la resolución decimal de cada variable para empaquetarla en una cantidad fija de bits.
3. **Mayor costo computacional en extremos de bajo consumo:** Requiere operaciones de bitwise (*bit-shifting*, máscaras, serialización a nivel de nibbles/bytes), lo que aumenta la complejidad del firmware.
4. **Fragilidad ante cambios de versión del protocolo:** Modificar el tamaño o significado de un campo requiere mecanismos de versionado (por ejemplo, sacrificar 1 o 2 bits de control para indicar versión de payload).
5. **Manejo de errores estricto:** La corrupción de un solo bit en un payload empaquetado puede alterar drásticamente múltiples variables simultáneamente (por ejemplo, el bit de signo o los bits compartidos entre temperatura y humedad).

---

### 6. ¿Cómo podemos hacer que el valor de temperatura quepa en 14 bits?

**Respuesta y Demostración Matemática:**
1. **Rango especificado:** $[0.00, 110.00] \text{ }^\circ\text{C}$ con precisión de 2 cifras decimales.
2. **Escalamiento a Entero:** Multiplicamos el valor flotante por $100$:
   $$\text{temp\_int} = \text{round}(\text{temperatura} \times 100)$$
   Esto mapea los valores al rango entero:
   $$0 \le \text{temp\_int} \le 11000$$
3. **Capacidad de 14 bits:**
   Un entero sin signo de 14 bits puede representar valores desde $0$ hasta:
   $$2^{14} - 1 = 16383$$
4. **Conclusión:**
   Como $11000 \le 16383$, el valor escalado cabe **perfectamente dentro de 14 bits** con margen de sobra (permite teóricamente temperaturas de hasta $163.83 \text{ }^\circ\text{C}$ sin alterar la estructura).
5. **Decodificación:**
   $$\text{temperatura} = \frac{\text{temp\_int}}{100.0}$$
   Se recupera el valor flotante con exactamente sus dos decimales originales sin pérdida de precisión.

---

### 7. ¿Qué sucedería si ahora la humedad también es tipo float con un decimal? ¿Qué decisiones tendríamos que tomar en ese caso?

**Análisis:**
- **Humedad original (entero [0, 100]):** Necesita 7 bits ($2^7 = 128 \ge 101$ estados).
- **Humedad como float con 1 decimal en [0.0, 100.0]:**
  - Mapeo escalado $\times 10$: entero de $0$ a $1000$ (1001 estados).
  - Bits necesarios: $\lceil \log_2(1001) \rceil = \mathbf{10 \text{ bits}}$ ($2^{10} = 1024 \ge 1001$).
  - Incremento requerido: $10 - 7 = \mathbf{3 \text{ bits adicionales}}$.

**Presupuesto de bits en 3 bytes (24 bits disponibles):**
$$\text{Temperatura (14b)} + \text{Humedad (10b)} + \text{Viento (3b)} = \mathbf{27 \text{ bits}}$$
¡Excede los 24 bits por 3 bits!

**Decisiones que tendríamos que tomar:**
1. **Opción A (Optimizar la Temperatura):**
   - El rango de temperatura $[0, 110.00]$ sólo usa hasta $11000$, pero $2^{14} = 16384$.
   - Si reducimos la resolución de la temperatura a 1 decimal (ej. $56.3 \text{ }^\circ\text{C}$):
     $$110.0 \times 10 = 1100 \implies \lceil \log_2(1101) \rceil = 11 \text{ bits}$$
     Ahorramos 3 bits ($14 - 11 = 3$).
     $$\text{Presupuesto nuevo: } 11\text{b (Temp)} + 10\text{b (Hum)} + 3\text{b (Viento)} = \mathbf{24 \text{ bits (3 bytes exactos)}}.$$
2. **Opción B (Compresión de rango de humedad):**
   - Si no se necesita el rango completo $[0, 100]\%$, o si se acota a $[20.0, 90.0]\%$ (rango meteorológico habitual), o se usa un paso de 0.2% en vez de 0.1%.
3. **Opción C (Transmisión alternada o mensaje de 2 paquetes):**
   - Enviar viento en mensajes alternos o cambiar a un payload de 4 bytes si el medio lo permite.
4. **Opción D (Codificación Aritmética / Compresión Combinada):**
   - Espacio total de estados: $11001 \times 1001 \times 8 = 88,096,008$ estados.
   - $\log_2(88,096,008) \approx 26.39$ bits. Ni siquiera con codificación aritmética cabe en 24 bits ($2^{24} = 16,777,216$ estados).
   - Por tanto, **la decisión obligatoria es reducir precisión** (ej. temperatura a 1 decimal en la Opción A).

---

### 8. ¿Qué parámetros o herramientas de Kafka podrían ayudarnos si las restricciones fueran aún más fuertes?

**Respuesta:**
1. **Compresión nativa a nivel de broker y productor (`compression.type`):**
   - Kafka soporta algoritmos de compresión como `zstd`, `lz4`, `snappy` y `gzip`.
   - `zstd` ofrece ratios de compresión excepcionales para lotes de mensajes (*batches*).
2. **Agrupamiento en lotes (*Batching*): `linger.ms` y `batch.size`:**
   - En lugar de enviar un mensaje por cada lectura (lo que genera overhead de encabezados TCP e IP), se configuran `linger.ms` (ej. 30000 ms) y `batch.size` para empaquetar múltiples lecturas de telemetría en un solo paquete comprimido, amortizando los headers.
3. **Serialización binaria con Schema Registry (Apache Avro / Protobuf):**
   - Uso de **Confluent Schema Registry** con Avro o Protocol Buffers. Estos eliminan nombres de campos y usan números de esquema pequeños (5 bytes de header) permitiendo serialización binaria altamente compacta y compatible con evolución de versiones.
4. **Kafka Compact Topics (`cleanup.policy=compact`):**
   - Permite retener únicamente el último valor conocido para cada `key` (ej. `sensor1`), ahorrando espacio de almacenamiento en el broker y reduciendo la carga de red cuando un consumidor se conecta a leer el estado actual.
5. **Gateway IoT Edge (MQTT / HTTP Proxy):**
   - Desplegar **Kafka REST Proxy** o un puente **MQTT-to-Kafka** en el nodo Edge. Los sensores transmiten usando protocolos de transporte ultralivianos (MQTT-SN o CoAP sobre UDP) hacia el Edge, y el Gateway se encarga de publicar en Kafka.
