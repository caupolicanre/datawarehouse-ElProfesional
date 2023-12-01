-------------------------------------------------------------
-- 					Dimensión Artículos
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Rubros (
	IDRubro INT PRIMARY KEY, -- INT para crear PKs compuestas por los Rubros y Subrubros
	nombre VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS Articulos (
	IDArticulo INT PRIMARY KEY, -- INT para crear PKs compuestas por los Códigos de Articulos y SubCódigos de Articulos
	nombre VARCHAR(100),
	rubro INT,
	FOREIGN KEY (rubro) REFERENCES Rubros(IDRubro)
);



-------------------------------------------------------------
-- 					Dimensión Orden
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Orden (
	NroOrden INT PRIMARY KEY,
	total_venta DECIMAL(10, 2)
);



-------------------------------------------------------------
-- 					Dimensión Clientes
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Localidades (
	IDLocalidad SERIAL PRIMARY KEY,
	nombre VARCHAR(100)
);


CREATE TABLE IF NOT EXISTS TipoCliente (
	IDTipoCliente SERIAL PRIMARY KEY,
	tipo_cliente VARCHAR(100)
);


CREATE TABLE IF NOT EXISTS Clientes (
	IDCliente INT PRIMARY KEY,
	razon_social VARCHAR(200),
	tipo_cliente INT,
	localidad INT,
	FOREIGN KEY (tipo_cliente) REFERENCES TipoCliente(IDTipoCliente),
	FOREIGN KEY (localidad) REFERENCES Localidades(IDLocalidad)
);



-------------------------------------------------------------
-- 					Dimensión Vendedores
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Vendedores (
	IDVendedor INT PRIMARY KEY,
	nombre VARCHAR(30)
);



-------------------------------------------------------------
-- 					Dimensión Tiempo
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Tiempo (
	IDFecha SERIAL PRIMARY KEY,
    Fecha TIMESTAMP,
	Periodo VARCHAR(20),
    Dia_nombre VARCHAR(20),
    DiaMes_numero SMALLINT,
    Mes_numero SMALLINT,
    Mes_nombre VARCHAR(20),
    Trimestre SMALLINT,
    Semestre SMALLINT,
    Anio SMALLINT
);



-------------------------------------------------------------
-- 					Tabla de Hechos "Renglon_Factura"
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Renglon_Factura (
	IDRenglon_Factura SERIAL PRIMARY KEY,
	IDFecha INT,
	IDArticulo INT,
	IDCliente INT,
	IDVendedor INT,
	NroOrden INT,
	total_venta_renglon DECIMAL(10, 2),
	cantidad_articulos_renglon INT,
	precio_unitario DECIMAL(12, 2),
	precio_unitario_iva DECIMAL(12, 2),
	FOREIGN KEY (IDFecha) REFERENCES Tiempo(IDFecha),
    FOREIGN KEY (IDArticulo) REFERENCES Articulos(IDArticulo),
    FOREIGN KEY (IDCliente) REFERENCES Clientes(IDCliente),
    FOREIGN KEY (IDVendedor) REFERENCES Vendedores(IDVendedor),
	FOREIGN KEY (NroOrden) REFERENCES Orden(NroOrden)
)