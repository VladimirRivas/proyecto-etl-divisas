# 💹 Global FX Insights: End-to-End Cloud Data Engineering

Este proyecto representa una solución integral de **Ingeniería de Datos** de nivel empresarial para el monitoreo y análisis de divisas. La arquitectura combina la potencia de la nube de **Microsoft Azure**, el procesamiento de **Databricks** y la automatización de **GitHub Actions** bajo un estándar de **DataOps**.

---

## ☁️ Ecosistema de Servicios en Microsoft Azure
La infraestructura está desplegada sobre el stack tecnológico de Azure, garantizando escalabilidad, seguridad y alta disponibilidad:

* **Azure Databricks**: Plataforma unificada de análisis donde se ejecuta el motor de procesamiento Spark para todas las transformaciones de datos.
* **Azure Data Lake Storage (ADLS Gen2)**: Almacenamiento jerárquico que actúa como el repositorio físico para las capas `bronze`, `silver` y `gold` del Data Lake.
* **Azure Key Vault**: Gestión centralizada y segura de secretos, protegiendo las credenciales de conexión (`DATABRICKS_TOKEN`) utilizadas en el pipeline de CI/CD.
* **Microsoft Entra ID (Azure AD)**: Control de identidades y acceso (IAM) para asegurar que solo usuarios autorizados interactúen con el Workspace.

---

## 🏗️ Arquitectura Medallion y Gobernanza con Unity Catalog
El proyecto implementa el patrón de diseño **Medallion**, gestionado íntegramente por **Unity Catalog (UC)** para una gobernanza centralizada de datos y metadatos:



* **Capa Bronze (Raw)**: Ingesta de datos crudos desde archivos CSV históricos y extracción incremental mediante la **API de Yahoo Finance**, orquestada por un archivo de configuración JSON modular.
* **Capa Silver (Cleansed)**: Proceso de refinamiento donde se normalizan esquemas, se gestionan datos nulos y se unifican las fuentes de datos en tablas Delta.
* **Capa Gold (Business)**: Capa de valor agregado donde se aplican **PySpark Window Functions** para calcular:
    * Medias Móviles Simples (SMA 30).
    * Bandas de Bollinger (Volatilidad).
    * Señales de Tendencia (*Bullish/Bearish/Neutral*).
* **Gobernanza de Datos (UC)**: Implementación de seguridad a nivel de catálogo mediante **Grants (DCL)**, asegurando que los permisos de lectura y escritura estén estrictamente controlados en cada esquema.

---

## 🤖 DevOps y Orquestación de Datos (CI/CD)
La entrega continua y la automatización son pilares de este proyecto:

* **GitHub Actions**: Pipeline de CI/CD que automatiza el despliegue de notebooks y la creación de **Databricks Jobs** mediante llamadas a la API de REST.
* **Databricks Workflows**: Orquestación de un DAG (Grafo Dirigido Acíclico) de 7 tareas secuenciales, asegurando que desde la preparación del ambiente (`PrepAmb`) hasta la auditoría final, el proceso sea resiliente y rastreable.



---

## 📊 Business Intelligence con Databricks AI/BI
Para la capa de consumo, se utilizó la herramienta moderna **AI/BI Dashboards** (antes Lakeview), permitiendo visualizaciones de alto impacto:

* **Monitor de Volatilidad**: Gráfico de líneas que visualiza el túnel de riesgo mediante las Bandas de Bollinger calculadas en la capa Gold.
* **KPIs de Acción**: Indicadores de tipo *Counter* para el precio actual, retorno diario y señales de trading automatizadas.
* **Radar de Anomalías (Scatter Plot)**: Análisis de dispersión que correlaciona el retorno diario frente al precio para identificar comportamientos atípicos en el mercado.



---
**Desarrollado por: Vladimir Rivas** 
