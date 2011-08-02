using System;
using System.Collections.Generic;
using System.Text;
using NavisionDB;
using System.Data;
using System.Configuration;
//
// Ponemos números de campo y tabla para prepararlo para la migración a 4.0 SP2
//
namespace MiddleWareTPVCentral
{
    public class TPV
    {

        private NavisionDBUser DBUser = null;
        private NavisionDBConnection Connection = null;

        public TPV(NavisionDBUser DBUser, NavisionDBConnection Connection)
        {
            this.DBUser = DBUser;
            this.Connection = Connection;
        }
        public DataSet Sincronizar_Vendedores(string CodTienda, string FechaD, string FechaH)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Vendedores()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();


                Dt.TableNo = 13; // Tabla Vendedores 
                Dt.AddColumn(1);   // Nº
                Dt.AddColumn(2);   // Nombre
                Dt.AddColumn(3);   // % Comisión
                Dt.AddColumn("Last Date Modified"); // Campo nuevo

                Dt.KeyInNavisionFormat = "Last Date Modified";
                Dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                //Dt.AddFilter("Shop Center Asigned", DBUser.UserCode);
                //Dt.AddFilter("Bloqueado", "false");

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Vendedores(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Recursos(string CodTienda, string FechaD, string FechaH)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Recursos()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 156; // Tabla Recursos 
                Dt.AddColumn(1);   // Nº
                Dt.AddColumn(2);   // Tipo (Persona,Máquina)
                Dt.AddColumn(3);   // Nombre
                Dt.AddColumn(18);   //Unidad medida base
                Dt.AddColumn(19);   //Coste unit. directo
                Dt.AddColumn(21);   //Coste unitario
                Dt.AddColumn(24);   //Precio venta
                Dt.AddColumn(26);   //Fecha últ. modificación
                Dt.AddColumn(51);   //Grupo contable producto
                Dt.AddColumn(58);   //Grupo registro IVA prod.


                Dt.KeyInNavisionFormat = "Responsibility Center,Last Date Modified";
                Dt.AddFilter("Responsibility Center", CodTienda);
                Dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                //Dt.AddFilter("Shop Center Asigned", DBUser.UserCode);
                //Dt.AddFilter("Bloqueado", "false");

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Recursos(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Variantes(string FechaD, string FechaH)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Variantes()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 5401; // Tabla variante producto 
                Dt.AddColumn(1);   // codigo
                Dt.AddColumn(2);   // nº producto
                Dt.AddColumn(3);   // descripcion
                Dt.AddColumn(4);   // descripcion2
                Dt.AddColumn(50010);   //
                Dt.AddColumn(50011);   //
                Dt.AddColumn(50100);   //
                Dt.AddColumn(50101);   //
                Dt.AddColumn(50102);   //
                Dt.AddColumn(50103);   //
                Dt.AddColumn(50104);   //
                //Dt.AddColumn(50105);   //
                Dt.AddColumn(50106);   //
                Dt.AddColumn(50107);   //
                Dt.AddColumn(50200);   //
                Dt.AddColumn(50201);   //
                Dt.AddColumn(50202);   //
                Dt.AddColumn(50203);   //

                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                // >> JDP añadido 20/05/2010
                Dt.AddFilter("Envio a Tiendas", "true");
                // << JDP añadido 20/05/2010
                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Variantes(): ", ex.Message);
            }
        }


        public DataSet Sincronizar_FPUrende(string CodTienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_FPUrende()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 60017; // Forma_Pago_Tienda 
                Dt.AddColumn(1);    //   
                Dt.AddColumn(2);    //  
                Dt.AddColumn(3);    //  
                Dt.AddColumn(4);    // 
                Dt.AddColumn(5);    //

                Dt.AddFilter("Centro_Responsabilidad", CodTienda);

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_FPUrende(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_CentroResponsabilidad(string CodTienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_CentroResponsabilidad()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 5714;      // Centro Responsabilidad 
                Dt.AddColumn(1);        // Code   
                Dt.AddColumn(14);       // Almacen 
                Dt.AddColumn(50000);    // Cuenta gastos de limpieza 
                Dt.AddColumn(50001);    // Cuenta gastos de papeleria
                Dt.AddColumn(50002);    //  Cuenta gastos varios
                Dt.AddColumn(50003);    // Sección regularizaciones
                Dt.AddColumn(50004);    // Sección inventario 
                Dt.AddColumn(50005);    // Cód. tarifa TPV
                Dt.AddColumn(50006);    // Nº cuenta caja central 
                Dt.AddColumn(50007);    // Cuenta traspaso de tienda
                Dt.AddColumn(50008);    // Banco traspaso de tienda 
                Dt.AddColumn(50009);    // Nombre libro diario
                Dt.AddColumn(50010);    // Nombre sección diario
                Dt.AddColumn(50011);    // Cargo para cheque regalo

                Dt.AddColumn(50012);    // Cód. cliente TPV 
                Dt.AddColumn(50013);    // Forma pago EFECTIVO
                Dt.AddColumn(50014);    // Forma pago VALES emitidos
                Dt.AddColumn(50015);    // Forma pago VALES recibidos
                Dt.AddColumn(50016);    // Forma pago TPV
                Dt.AddColumn(50017);    // Forma pago liq. entregas
                Dt.AddColumn(50018);    // Forma pago cheque regalo
                Dt.AddColumn(50019);    // Forma pago dto. empleados
                Dt.AddColumn(50020);    // Forma pago crédito
                Dt.AddColumn(50021);    // Forma pago financiación
                Dt.AddColumn(50022);    // Forma pago dto. colectivos
                Dt.AddColumn(50023);    // Forma pago por nómina
                Dt.AddColumn(50024);    // Cuenta gastos financiación
                Dt.AddColumn(50025);    // Nº cuenta diferencias caja
                Dt.AddColumn(50026);    //Gastos de tintorería
                Dt.AddColumn(50027);    //Gastos de material de limpieza
                Dt.AddColumn(50028);    //Gastos de material de oficina
                Dt.AddColumn(50029);    //Gastos de mensajero.
                Dt.AddColumn(50030);    //Gastos de modista.
                Dt.AddColumn(50031);    //Gastos de otros gastos varios tienda
                Dt.AddColumn(50032);    //Gastos reparación clazado
                Dt.AddColumn(50033);    //Gastos arreglos en tienda
                Dt.AddColumn(50034);    //Gastos de arreglo bisutería.

                //>>ICP.EB_20100413

                Dt.AddColumn(50061); //"Forma de pago tarjeta regalo"
                //Dt.AddColumn(50062); //"Nº serie activaciones TR" // No se usa, se configura a mano
                //Dt.AddColumn(50063); //"Nº serie desactivaciones TR" // No se usa, se configura a mano
                Dt.AddColumn(50064); //"URL WebServices TR"
                Dt.AddColumn(50065); //"Cuenta tarjeta regalo"

                //>>ICP.EB_20100413

                Dt.AddFilter("Code", CodTienda);

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_CentroResponsabilidad(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_AlmPrincipal(string CodTienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_AlmPrincipal()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 14;      // Almacen
                Dt.AddColumn(1);        // Code   
                //Dt.AddColumn(14);       // Almacen 
                Dt.AddColumn(50000);    // Cuenta gastos de limpieza 
                Dt.AddColumn(50001);    // Cuenta gastos de papeleria
                Dt.AddColumn(50002);    //  Cuenta gastos varios
                Dt.AddColumn(50003);    // Sección regularizaciones
                Dt.AddColumn(50004);    // Sección inventario 
                Dt.AddColumn(50005);    // Cód. tarifa TPV
                Dt.AddColumn(50006);    // Nº cuenta caja central 
                Dt.AddColumn(50007);    // Cuenta traspaso de tienda
                Dt.AddColumn(50008);    // Banco traspaso de tienda 
                Dt.AddColumn(50009);    // Nombre libro diario
                Dt.AddColumn(50010);    // Nombre sección diario
                Dt.AddColumn(50011);    // Cargo para cheque regalo

                Dt.AddColumn(50012);    // Cód. cliente TPV 
                Dt.AddColumn(50013);    // Forma pago EFECTIVO
                Dt.AddColumn(50014);    // Forma pago VALES emitidos
                Dt.AddColumn(50015);    // Forma pago VALES recibidos
                Dt.AddColumn(50016);    // Forma pago TPV
                Dt.AddColumn(50017);    // Forma pago liq. entregas
                Dt.AddColumn(50018);    // Forma pago cheque regalo
                Dt.AddColumn(50019);    // Forma pago dto. empleados
                Dt.AddColumn(50020);    // Forma pago crédito
                Dt.AddColumn(50021);    // Forma pago financiación
                Dt.AddColumn(50022);    // Forma pago dto. colectivos
                Dt.AddColumn(50023);    // Forma pago por nómina
                Dt.AddColumn(50024);    // Cuenta gastos financiación
                Dt.AddColumn(50025);    // Nº cuenta diferencias caja

                Dt.AddFilter("Code", CodTienda);

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_AlmPrincipal(): ", ex.Message);
            }
        }


        public DataSet Sincronizar_FormasPago()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_FormasPago()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 289; // Tabla Formas Pago 
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);   //  
                Dt.AddColumn(4);   // 

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_FormasPago(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Temporada(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Temporada()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50100; // Temporad
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);   //  
                Dt.AddColumn(4);   // 
                Dt.AddColumn(5);
                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Temporada(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Color(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Color()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50101; // Color
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);   //  
                Dt.AddColumn(4);
                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));


                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Color(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_FamProd(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_FamProd()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50103; // Familia producto
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //
                Dt.AddColumn(3);

                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_FamProd(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_SubFamProd(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_SubFamProd()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50104; // SubFamilia producto
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);
                Dt.AddColumn(4);

                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_SubFamProd(): ", ex.Message);
            }
        }


        public DataSet Sincronizar_Composicion(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Composicion()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50105; // Composicion
                Dt.AddColumn(10);   //   
                Dt.AddColumn(20);   //  
                Dt.AddColumn(21);
                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Composicion(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Coleccion(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Coleccion()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50107; // Coleccion
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);
                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Coleccion(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_LinProducto(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_LinProducto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50108; // Linea producto
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);
                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_LinProducto(): ", ex.Message);
            }
        }


        public DataSet Sincronizar_Tallaje(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Tallaje()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50102; // Tallaje
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);   //  
                Dt.AddColumn(4);   //  
                Dt.AddColumn(5);   //  
                Dt.AddColumn(6);   //  
                Dt.AddColumn(7);   //  
                Dt.AddColumn(8);   //  
                Dt.AddColumn(9);   //  
                Dt.AddColumn(10);   //  
                Dt.AddColumn(11);   //  
                Dt.AddColumn(12);   //  
                Dt.AddColumn(13);   //  
                Dt.AddColumn(14);   //  
                Dt.AddColumn(15);   //  
                Dt.AddColumn(16);   //  
                Dt.AddColumn(17);   //  
                Dt.AddColumn(18);   //  
                Dt.AddColumn(19);   //  
                Dt.AddColumn(20);   //  
                Dt.AddColumn(21);   //  
                Dt.AddColumn(22);   //  
                Dt.AddColumn(23);
                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Tallaje(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_ProdTallaColor(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_ProdTallaColor()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50106; // Producto Talla Color
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);   //  
                Dt.AddColumn(4);   //  
                Dt.AddColumn(5);   //  
                Dt.AddColumn(6);   //  
                Dt.AddColumn(7);   //  
                Dt.AddColumn(8);   //  
                Dt.AddColumn(9);   //  
                Dt.AddColumn(10);   //  
                Dt.AddColumn(11);   //  
                Dt.AddColumn(12);   //  
                Dt.AddColumn(13);   //  
                Dt.AddColumn(14);   //  
                Dt.AddColumn(15);   //  
                Dt.AddColumn(16);   //  
                Dt.AddColumn(17);   //  
                Dt.AddColumn(18);   //  
                Dt.AddColumn(19);   //  
                Dt.AddColumn(20);   //  
                Dt.AddColumn(21);   //  
                Dt.AddColumn(22);   //  
                Dt.AddColumn(23);   //   
                Dt.AddColumn(24);   //  
                Dt.AddColumn(25);   //  
                Dt.AddColumn(26);   //  
                Dt.AddColumn(27);   //  
                Dt.AddColumn(28);   //  
                Dt.AddColumn(29);   //  
                Dt.AddColumn(30);   //  
                Dt.AddColumn(31);   //  
                Dt.AddColumn(32);   //  
                Dt.AddColumn(33);   //  
                Dt.AddColumn(34);   //  
                Dt.AddColumn(35);   //  
                Dt.AddColumn(36);   //  
                Dt.AddColumn(37);   //  
                Dt.AddColumn(38);   //  
                Dt.AddColumn(39);   //  
                Dt.AddColumn(40);   //  
                Dt.AddColumn(41);   //  
                Dt.AddColumn(42);   //  
                Dt.AddColumn(43);   //  
                Dt.AddColumn(44);   //  
                Dt.AddColumn(46);   //  
                Dt.AddColumn(47);   //  
                Dt.AddColumn(48);   //  
                Dt.AddColumn(49);   //  
                Dt.AddColumn(51);   //  
                Dt.AddColumn(52);   //  
                Dt.AddColumn(53);   // 
                Dt.AddColumn(54);
                Dt.KeyInNavisionFormat = "Fecha ult. modificación";
                Dt.AddFilter("Fecha ult. modificación", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ProdTallaColor(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Cuentas()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Cuentas()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 15;   // Tabla Cuenta 
                Dt.AddColumn(1);   //   Nº
                Dt.AddColumn(2);   //   NOMBRE
                Dt.AddColumn(3);   //   ALIAS
                Dt.AddColumn(4);   //   Tipo mov. (Auxiliar,Mayor|Posting,Heading)
                Dt.AddColumn(9);   //   Comercial/Balance (Comercial,Balance|Income Statement,Balance Sheet)
                Dt.AddColumn(13);   //  Bloqueado
                Dt.AddColumn(19);   //  Indentar
                Dt.AddColumn(26);   //  Fecha últ. modificación
                Dt.AddColumn(34);   //  Sumatorio
                Dt.AddColumn(43);   //  Tipo IVA ( ,Compras,Ventas| ,Purchase,Sale)
                Dt.AddColumn(44);   //  Grupo contable negocio
                Dt.AddColumn(45);   //  Grupo contable producto
                Dt.AddColumn(57);   //  Grupo registro IVA neg.
                Dt.AddColumn(58);   //  Grupo registro IVA prod.

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Cuentas(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrupoContableCliente()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrupoContableCliente()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 92;    // Tabla Formas Pago 
                Dt.AddColumn(1);    //   
                Dt.AddColumn(2);    //  
                Dt.AddColumn(50000);// Cta. anticipos clientes

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrupoContableCliente(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrupoContableExistencias()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrupoContableExistencias()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 94;    // GRUPO CONTABLE EXISTENCIAS 
                Dt.AddColumn(1);    //   Code
                Dt.AddColumn(2);    //   Description

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrupoContableExistencias(): ", ex.Message);
            }
        }


        public DataSet Sincronizar_Secciones()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Secciones()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 60001;        // Secciones
                Dt.AddColumn(1);          //Codigo
                Dt.AddColumn(2);          //Descripcion
                Dt.AddColumn(3);          //Internet

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Secciones(): ", ex.Message);
            }
        }

        //DEPRECATED
        public DataSet Sincronizar_GrupoVentas()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrupoVentas()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableName = "Grupo_Ventas";        // Grupo_Ventas 
                Dt.AddColumn(1);          //Codigo
                Dt.AddColumn(2);          //Descripcion
                Dt.AddColumn(3);          //Tipo_Comision

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrupoVentas(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Familia()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Familia()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 5722;        //CATEGORIA PRODUCTO = FAMILIA 
                Dt.AddColumn(1);          //Code
                Dt.AddColumn(3);          //Descripcion
                Dt.AddColumn(4);          //Grupo regis. prod. genérico
                Dt.AddColumn(5);          //Grupo regis. invent. genérico
                Dt.AddColumn(6);          //Cód. grupo impuesto gen.
                Dt.AddColumn(7);          //Método coste genér.
                Dt.AddColumn(8);          //Grupo reg. IVA prod. genér.
                //Dt.AddColumn(60000);          //Cód. Sección
                //Dt.AddColumn(60001);          //Familia Música

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Familia(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_SubFamilia()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_SubFamilia()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 5723;        //Grupo producto = SUBFAMILIA
                Dt.AddColumn(1);          //Cód. categoría producto
                Dt.AddColumn(2);          //Código  
                Dt.AddColumn(3);          //Descripción

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_SubFamilia(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_DtoEmpleado()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_DtoEmpleado()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 60045;        //Dtos_Empleados
                Dt.AddColumn(1);          //Cod_empleado
                Dt.AddColumn(2);          //Cod_Marca
                Dt.AddColumn(3);          //Cod_Seccion
                Dt.AddColumn(4);          //Cod_Familia
                Dt.AddColumn(5);          //Cod_Subfamilia
                Dt.AddColumn(6);          //Cod_Producto
                Dt.AddColumn(7);          //Descuento

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_DtoEmpleado(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Colectivos()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Colectivos()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 60046;        //Colectivos
                Dt.AddColumn(1);          //Cod_Tienda
                Dt.AddColumn(2);          //Cod_Colectivo
                Dt.AddColumn(3);          //Nombre

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Colectivos(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_DtoColectivos()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_DtoColectivos()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 60047;        //Dto. Colectivos
                Dt.AddColumn(1);          //Cod_Colectivo
                Dt.AddColumn(2);          //Cod_Marca
                Dt.AddColumn(3);          //Cod_Seccion
                Dt.AddColumn(4);          //Cod_Familia
                Dt.AddColumn(5);          //Cod_Subfamilia
                Dt.AddColumn(6);          //Cod_Producto
                Dt.AddColumn(7);          //Descuento

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_DtoColectivos(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_ProductosFinancieros()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_ProductosFinancieros()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 60028;        //PRODUCTOS FINANCIEROS
                Dt.AddColumn(1);          //Cod_Financiera
                Dt.AddColumn(2);          //Formula
                Dt.AddColumn(3);          //Descripcion
                Dt.AddColumn(5);          //%_Apertura
                Dt.AddColumn(6);          //Importe_Minimo
                Dt.AddColumn(7);          //Activo
                Dt.AddColumn(4);          //%_Gastos

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ProductosFinancieros(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Banco()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Banco()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 270;        //BANCO 
                Dt.AddColumn(1);         //Code
                Dt.AddColumn(2);         //Nombre
                Dt.AddColumn(5);         //Dirección
                Dt.AddColumn(7);         //Población
                Dt.AddColumn(8);         //Contacto
                Dt.AddColumn(9);         //Nº teléfono
                Dt.AddColumn(13);        //Cód. cuenta banco
                Dt.AddColumn(21);        //Grupo contable banco
                Dt.AddColumn(39);        //Bloqueado
                Dt.AddColumn(91);        //C.P.
                Dt.AddColumn(92);        //Provincia
                Dt.AddColumn(101);        //Cód. sucursal banco
                Dt.AddColumn(10700);        //CCC Cód. banco
                Dt.AddColumn(10701);        //CCC Cód. oficina
                Dt.AddColumn(10702);        //CCC Dígito control
                Dt.AddColumn(10703);        //CCC Nº cuenta
                Dt.AddColumn(10704);        //Nº CCC
                Dt.AddColumn(7000016);        //CIF/NIF

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Banco(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrupoContableBanco()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrupoContableBanco()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 277;       // GRUPO CONTABLE BANCO 
                Dt.AddColumn(1);        //Code
                Dt.AddColumn(2);        //Cta. banco
                Dt.AddColumn(7000000);  //Cta. deudas efecs. descontados
                Dt.AddColumn(7000001);  //Cta. servicios bancarios
                Dt.AddColumn(7000002);  //Cta. intereses descuento
                Dt.AddColumn(7000003);  //Cta. gastos impagados
                Dt.AddColumn(7000004);  //Cta. deudas facts. descontadas

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrupoContableBanco(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrupoContableProveedor()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrupoContableProveedor()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 93;   //Grupo contable proveedor
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrupoContableProveedor(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrupoDescuentoCliente()
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_GrupoDescuentoCliente()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                dt.TableNo = 340; //Grupo descuento cliente
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.KeyInNavisionFormat = "Code";
                da.AddTable(dt);
                da.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrupoDescuentoCliente()", exception.Message);
            }
        }


        public DataSet Sincronizar_GrupoDescuentoProducto()
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_GrupoDescuentoProducto()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                dt.TableNo = 341; //Grupo descuento producto
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.KeyInNavisionFormat = "Code";
                da.AddTable(dt);
                da.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrupoDescuentoProducto()", exception.Message);
            }
        }

        public DataSet Sincronizar_Empleados(string CodTienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_Empleados()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 5200;   //Empleado
                Dt.AddColumn(1);   //   
                Dt.AddColumn(2);   //  
                Dt.AddColumn(3);   //   
                Dt.AddColumn(4);   // 
                Dt.AddColumn(5);   //   
                Dt.AddColumn(6);   //  
                Dt.AddColumn(7);   //   
                Dt.AddColumn(8);   // 
                Dt.AddColumn(10);   //   
                Dt.AddColumn(11);   // 
                Dt.AddColumn(12);   //   
                Dt.AddColumn(13);   //  
                Dt.AddColumn(14);   //   
                Dt.AddColumn(15);   // 
                Dt.AddColumn(20);   //  
                Dt.AddColumn(21);   //   
                Dt.AddColumn(24);   // 
                Dt.AddColumn(25);   //   
                Dt.AddColumn(29);   // 
                Dt.AddColumn(52);   //   
                Dt.AddColumn(38);   // 
                Dt.AddColumn(60000);   //   
                Dt.AddColumn(60001);   // 
                Dt.AddColumn(60003);   //   
                Dt.AddColumn(60004);   // 
                Dt.KeyInNavisionFormat = "Global Dimension 1 Code";

                Dt.AddFilter("Global Dimension 1 Code", CodTienda + "|''");                
                da.AddTable(Dt);

                da.Fill(ref ds, false);

                ds.Tables[0].Columns.Add("NumVend", System.Type.GetType("System.String"));
                ds.Tables[0].Columns.Add("NombreVend", System.Type.GetType("System.String"));
                ds.Tables[0].Columns.Add("Comision", System.Type.GetType("System.Decimal"));
                ds.Tables[0].Columns.Add("FechaModificado", System.Type.GetType("System.DateTime"));

                for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                {
                    if (ds.Tables[0].Rows[i]["EmployeeCod_Vendedor"].ToString() != "")
                    {
                        NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                        NavisionDBDataReader rd;

                        Dt.Reset();
                        Dt.TableNo = 13;                         //Salesperson/Purchaser
                        Dt.AddColumn(1);                         // Nº
                        Dt.AddColumn(2);                         // Nombre
                        Dt.AddColumn(3);                         // % Comisión
                        Dt.AddColumn("Last Date Modified");      // Campo nuevo

                        Dt.AddFilter("Code", ds.Tables[0].Rows[i]["EmployeeCod_Vendedor"].ToString());

                        //Dt.AddFilter(4, "Bar Code");                            //Cross-Reference Type
                        Cmd.Table = Dt;
                        rd = Cmd.ExecuteReader(false);
                        if (rd.RecordsAffected > 0)
                        {
                            ds.Tables[0].Rows[i]["NumVend"] = rd.GetString(0);
                            ds.Tables[0].Rows[i]["NombreVend"] = rd.GetString(1);
                            ds.Tables[0].Rows[i]["Comision"] = rd.GetDecimal(2);
                            ds.Tables[0].Rows[i]["FechaModificado"] = rd.GetDataTime(3);
                        }
                    }
                }


                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001
                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Empleados(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrContableNegocio()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrContableNegocio()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 250;   // Tabla Formas Pago 
                Dt.AddColumn(1);    // Código  
                Dt.AddColumn(2);    // Descripción 
                Dt.AddColumn(3);    // Grupo reg. IVA neg. genérico  
                Dt.AddColumn(4);    // Inserta genérico auto. 

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrContableNegocio(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrContableProducto()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrContableProducto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 251;   // Tabla Formas Pago 
                Dt.AddColumn(1);    // Código  
                Dt.AddColumn(2);    // Descripción 
                Dt.AddColumn(3);    // Grupo reg. IVA prod. genérico
                Dt.AddColumn(4);    // Inserta genérico auto. 

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrContableProducto(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_ConfGrContable()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_ConfGrContable()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 252;   // Tabla Formas Pago 
                Dt.AddColumn(1);    // Grupo contable negocio 
                Dt.AddColumn(2);    // Grupo contable producto
                Dt.AddColumn(10);   // Cta. venta
                Dt.AddColumn(11);   // Cta. dto. línea ventas 
                Dt.AddColumn(12);   // Cta. dto. factura ventas 
                Dt.AddColumn(13);   // Cta. dto. P.P. ventas
                Dt.AddColumn(27);   // Cta. abono ventas 

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ConfGrContable(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrIVANegocio()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrIVANegocio()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();

                DataSet ds = new DataSet();

                Dt.TableNo = 323;   // Tabla Grupo registro IVA negocio 
                Dt.AddColumn(1);    // Código  
                Dt.AddColumn(2);    // Descripción 


                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrIVANegocio(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_GrIVAProducto()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_GrIVAProducto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();

                DataSet ds = new DataSet();

                Dt.TableNo = 324;   // Tabla Grupo registro IVA producto 
                Dt.AddColumn(1);    // Código  
                Dt.AddColumn(2);    // Descripción 


                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrIVAProducto(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_ConfGrIVA()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_ConfGrIVA()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();

                DataSet ds = new DataSet();

                Dt.TableNo = 325;   // Tabla Grupo registro IVA producto 
                Dt.AddColumn(1);    // Grupo registro IVA neg.  
                Dt.AddColumn(2);    // Grupo registro IVA prod. 
                Dt.AddColumn(3);    // Tipo cálculo IVA  
                Dt.AddColumn(4);    // % IVA+RE 
                Dt.AddColumn(5);    // Tipo IVA no realizado  
                Dt.AddColumn(6);    // Ajuste para dto. p.p.
                Dt.AddColumn(7);    // Cta. IVA repercutido  
                Dt.AddColumn(8);    // Cta. IVA reper. no realizado 
                Dt.AddColumn(9);    // Cta. IVA soportado  
                Dt.AddColumn(10);   // Cta. IVA sopor. no realizado
                Dt.AddColumn(11);   // Cta. reversión IVA
                Dt.AddColumn(12);   // Cta. reversión IVA no realiz.
                Dt.AddColumn(10700);   // % RE 
                Dt.AddColumn(10701);   // % IVA

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ConfGrIVA(): ", ex.Message);
            }
        }

        public DataSet Sincronizar_Clientes(string FechaD, string FechaH, string CodCliGen)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_Clientes()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableName = "Customer";
                dt.AddColumn("No.");         
                dt.AddColumn("Name");        
                dt.AddColumn("Address");     
                dt.AddColumn("Contact");     
                dt.AddColumn("Post Code");   
                dt.AddColumn("City");        
                dt.AddColumn("County");      
                dt.AddColumn("Phone No.");   
                dt.AddColumn("E-Mail");      
                dt.AddColumn("VAT Registration No.");         
                dt.AddColumn("Payment Method Code");         
                dt.AddColumn("Payment Terms Code");         
                dt.AddColumn("Customer Price Group");       
                dt.AddColumn("Prices Including VAT");       
                dt.AddColumn("Customer Disc. Group");
                dt.AddColumn("Last Date Modified");
                dt.AddColumn("Cliente Generico");

                //dt.KeyInNavisionFormat = "Last Date Modified";
                //dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                //SOLO SINCRONIZAMOS EL CLIENTE GENERICO
                //EL RESTO DE CLIENTES SE SINCRONIZAN INDIVIDUALMENTE DESDE LOS TICKET
                dt.AddFilter("No.", CodCliGen);
                dt.AddFilter("Blocked", " ");

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Clientes()", ex.Message);
            }
        }

        public DataSet Sincronizar_ProductosPack(string FechaD, string FechaH, string CodTienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_ProductosPack()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 27;    //PRODUCTOS
                dt.AddColumn(1);    // Nº                                    
                dt.AddColumn(3);    // Descripción  
                dt.AddColumn(8);   // Unidad de Medida Base                                    
                dt.AddColumn(11);    // Grupo contable existencias                                     
                dt.AddColumn(18);   // Precio venta                                     
                dt.AddColumn(91);   // Grupo contable producto
                dt.AddColumn(99);     // Grupo registro IVA prod.
                dt.AddColumn(5702);     //Familia
                dt.AddColumn(5704);     //Subfamilia
                dt.AddColumn(57000);    //Fecha inicio
                dt.AddColumn(57001);    //Fecha fin
                dt.AddColumn(57002);    //Productos Opcionales
                dt.AddColumn(57003);    //Tipo Pack

                //dt.AddColumn(60000);    //Calificacion
                //dt.AddColumn(60001);    //Tipo_Etiqueta
                //dt.AddColumn(60002);    //Tipo_Articulo
                //dt.AddColumn(60003);    //Cod_Autor
                //dt.AddColumn(60004);    //Cod_Seccion
                //dt.AddColumn(50003);    //Producto por seccion

                dt.AddColumn("Last Date Modified");
                dt.KeyInNavisionFormat = "Producto pack,Cód. tienda,Last Date Modified";
                dt.AddFilter("Producto pack", "true");
                //dt.AddFilter("Cód. tienda"," ");
                if (CodTienda == "")
                    CodTienda = "kk";
                dt.AddFilter("Cód. tienda", CodTienda + "|''");
                dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                //dt.AddFilter("Blocked", "false");

                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 90;                         //LISTA DE MATERIALES DE PACK
                dt.AddColumn(1);                           //Nº L.M.
                dt.AddColumn(2);                           //Nº línea
                dt.AddColumn(3);                           //Tipo
                dt.AddColumn(4);                           //Nº producto
                dt.AddColumn(6);                            //Descripción
                dt.AddColumn(7);                           //Cód. unidad medida
                dt.AddColumn(8);                            //Cantidad por
                dt.AddColumn(50000);                           //<Coste>
                dt.AddColumn(50001);                            //<Coste total>
                dt.AddColumn(50002);                           //<Obligatorio venta en pack>
                dt.AddColumn(50003);                            //<Tipo Obligatorio/Opcional>

                da.AddTable(dt);
                da.AddRelation("Item", "No.", "BOM Component", "Parent Item No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ProductosPack()", ex.Message);
            }
        }

        public DataSet Sincronizar_Productos(string FechaD, string FechaH, string FiltroNoProducto)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_Productos()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 27; //Producto
                table.AddColumn(1);
                table.AddColumn(3);
                table.AddColumn(4);
                table.AddColumn(5);
                table.AddColumn(8);
                table.AddColumn(11);
                table.AddColumn(14);
                table.AddColumn(18);
                table.AddColumn(31);
                table.AddColumn(32);
                table.AddColumn(91);
                table.AddColumn(99);
                table.AddColumn(50003);
                table.AddColumn(50100);
                table.AddColumn(50101);
                table.AddColumn(50102);
                table.AddColumn(50103);
                table.AddColumn(50104);
                table.AddColumn(50105);
                table.AddColumn(50106);
                table.AddColumn(50111);
                table.AddColumn(50114);
                table.AddColumn(50115);
                table.AddColumn(50116);

                //>>ICP.EB_20100413

                table.AddColumn(50009); //"Tarjeta regalo"
                table.AddColumn(50010); //"Importe tarjeta regalo"
                table.AddColumn(50011); //"País tarjeta regalo"

                //>>ICP.EB_20100413

                table.AddColumn("Last Date Modified");
                table.KeyInNavisionFormat = "Last Date Modified";
                table.AddFilter("Last Date Modified", Utilidades.FechaDesde(FechaD) + ".." + Utilidades.FechaHasta(FechaH));
                table.AddFilter("Blocked", "false");
                table.AddFilter("Envio a Tiendas", "true");
                table.AddFilter(1, FiltroNoProducto);
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Productos()", exception.Message);
            }
        }



        public DataSet Sincronizar_Productos(string FechaD, string FechaH)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_Productos()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 27; //Producto
                dt.AddColumn(1);    // Nº                                    
                dt.AddColumn(3);    // Descripción  
                dt.AddColumn(4);    // Descripción alias                                    
                dt.AddColumn(5);    // Descripción 2                                                                                    
                dt.AddColumn(8);   // Unidad de Medida Base                                    
                dt.AddColumn(11);    // Grupo contable existencias                                                                    
                dt.AddColumn(14);  // Cód. dto. producto/cliente                                    
                dt.AddColumn(18);   // Precio venta                                     
                dt.AddColumn(31);   // Nº proveedor                                     
                dt.AddColumn(32);   // Cód. producto proveedor                                     
                dt.AddColumn(91);   // Grupo contable producto
                dt.AddColumn(99);     // Grupo registro IVA prod.
                dt.AddColumn(50003);    //Producto por seccion
                dt.AddColumn(50100);
                dt.AddColumn(50101);
                dt.AddColumn(50102);
                dt.AddColumn(50103);
                dt.AddColumn(50104);
                dt.AddColumn(50105);
                dt.AddColumn(50106);
                dt.AddColumn(50111);
                dt.AddColumn(50114);
                dt.AddColumn(50115);
                dt.AddColumn(50116);
                dt.AddColumn(50009); //"Tarjeta regalo"
                dt.AddColumn(50010); //"Importe tarjeta regalo"
                dt.AddColumn(50011); //"País tarjeta regalo"
                dt.AddColumn("Last Date Modified");

                dt.KeyInNavisionFormat = "Last Date Modified";

                dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                dt.AddFilter("Blocked", "false");
                dt.AddFilter("Envio a Tiendas", "true");                

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Productos()", ex.Message);
            }
        }

        public DataSet Sincronizar_Variantes(string FechaD, string FechaH, string FiltroNoProducto)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_Variantes()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 5401; //Variante producto
                table.AddColumn(1);
                table.AddColumn(2);
                table.AddColumn(3);
                table.AddColumn(4);
                table.AddColumn(50010);
                table.AddColumn(50011);
                table.AddColumn(50100);
                table.AddColumn(50101);
                table.AddColumn(50102);
                table.AddColumn(50103);
                table.AddColumn(50104);
                table.AddColumn(50106);
                table.AddColumn(50107);
                table.AddColumn(50200);
                table.AddColumn(50201);
                table.AddColumn(50202);
                table.AddColumn(50203);
                table.KeyInNavisionFormat = "Fecha ult. modificación";
                table.AddFilter("Fecha ult. modificación", Utilidades.FechaDesde(FechaD) + ".." + Utilidades.FechaHasta(FechaH));
                table.AddFilter("Envio a Tiendas", "true");
                table.AddFilter("Item No.", FiltroNoProducto);
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Variantes(): ", exception.Message);
            }
        }
        public DataSet Sincronizar_TarifaVentaProducto(string FechaD, string FechaH, string CodTarifa, string PrecioIni, string FiltroNoProducto)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_TarifaVentaProducto()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 7002; //Precio venta (Tarifas)
                table.AddColumn(1);
                table.AddColumn(2);
                table.AddColumn(3);
                table.AddColumn(4);
                table.AddColumn(5);
                table.AddColumn(7);
                table.AddColumn(10);
                table.AddColumn(13);
                table.AddColumn(7001);
                table.AddColumn(11);
                table.AddColumn(5400);
                table.AddColumn(5700);
                table.AddColumn("Last Date Modified");
                table.AddColumn("PrecioIniciTPV");
                table.AddColumn("Ending Date");
                table.KeyInNavisionFormat = "PrecioIniciTPV,Last Date Modified";
                //>>EB.ICP.20100531
                //table.AddFilter("PrecioIniciTPV", PrecioIni);
                //<<EB.ICP.20100531
                table.AddFilter("Last Date Modified", Utilidades.FechaDesde(FechaD) + ".." + Utilidades.FechaHasta(FechaH));
                table.AddFilter("Item No.", FiltroNoProducto);
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                try
                {
                    this.Connection.close();
                    this.Connection.Dispose();
                    this.Connection = Utilidades.Conectar_Navision();
                }
                catch (Exception ex2)
                {
                    Console.WriteLine(ex2.Message);
                }
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_TarifaVentaProducto()", exception.Message);
            }
        }
        public DataSet Sincronizar_DtoLinea(string FDesde, string FHasta)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_DtoLinea()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 7004; //Descuento línea venta
                table.AddColumn(1);
                table.AddColumn(2);
                table.AddColumn(3);
                table.AddColumn(4);
                table.AddColumn(5);
                table.AddColumn(13);
                table.AddColumn(14);
                table.AddColumn(15);
                table.AddColumn(21);
                table.AddColumn(5400);
                table.AddColumn(5700);
                table.AddColumn(50000);
                table.KeyInNavisionFormat = "Last Date Modified";
                table.AddFilter("Last Date Modified", Utilidades.FechaDesde(FDesde) + ".." + Utilidades.FechaHasta(FHasta));
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_DtoLinea()", exception.Message);
            }
        }
        public DataSet Recepcion_Transferencia_Pendiente(DataSet Lineas)
        {
            bool cdadPendienteSuficiente = true;
            DataSet set = new DataSet();
            NavisionDBAdapter adapterSet = new NavisionDBAdapter();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Recepcion_Transferencia_Pendiente()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                if ((Lineas.Tables.Count > 0) && (Lineas.Tables[0].Rows.Count > 0))
                {
                    for (int num3 = 0; num3 <= (Lineas.Tables[0].Rows.Count - 1); num3++)
                    {
                        string noDocumento = Lineas.Tables[0].Rows[num3][0].ToString();
                        string noLinea = Lineas.Tables[0].Rows[num3][1].ToString();
                        decimal cdadPendienteTPV = decimal.Parse(Lineas.Tables[0].Rows[num3][2].ToString());
                        if (!this.LinTransferenciaTieneSuficienteCdadPendiente(noDocumento, noLinea, cdadPendienteTPV))
                        {
                            cdadPendienteSuficiente = false;
                        }
                    }
                }
                return Utilidades.GenerarResultado(cdadPendienteSuficiente.ToString());
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Recepcion_Transferencia_Pendiente()", ex.Message);
            }
        }
        private bool LinTransferenciaTieneSuficienteCdadPendiente(string noDocumento, string noLinea, decimal cdadPendienteTPV)
        {
            bool cdadSuficiente = false;
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            table.TableNo = 5741; //Lín. transferencia
            table.AddColumn(1);
            table.AddColumn(2);
            table.AddColumn(34);
            table.AddFilter(1, noDocumento);
            table.AddFilter(2, noLinea);
            adapter.AddTable(table);
            adapter.Fill(ref ds, false);
            Utilidades.CompletarDataSet(ref ds, false, false);
            if (ds.Tables[0].Rows.Count != 0)
            {
                cdadSuficiente = cdadPendienteTPV <= decimal.Parse(ds.Tables[0].Rows[0][2].ToString());
            }
            return cdadSuficiente;
        }
        public string DatasetToCSV(DataSet ds, char separator)
        {
            StringBuilder str = new StringBuilder();
            bool IsNewRow;

            foreach (DataRow dr in ds.Tables[0].Rows)
            {
                IsNewRow = true;

                foreach (object field in dr.ItemArray)
                {
                    if (IsNewRow)
                    {
                        IsNewRow = false;
                        str.Append("|" + field.ToString() + "|");
                    }
                    else
                    {
                        str.Append(separator + "|" + field.ToString() + "|");
                    }
                }
                str.Append('\n');
            }
            return str.ToString();
        }

        public DataSet Sincronizar_TarifaVentaProducto_Dataport(string FechaD, string FechaH, string CodTarifa, string PrecioIni)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_TarifaVentaProducto_Dataport()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                DataSet dsSincronizarTarifasVenta = new DataSet();
                DataSet dsCSV = new DataSet();
                dsSincronizarTarifasVenta = this.Sincronizar_TarifaVentaProducto(FechaD, FechaH, CodTarifa, PrecioIni);
                string csv = this.DatasetToCSV(dsSincronizarTarifasVenta, ';');
                dsCSV.Tables.Clear();
                dsCSV.Tables.Add("TarifasCSV");
                dsCSV.Tables[0].Columns.Add("CSVValue");
                DataRow dr = dsCSV.Tables["TarifasCSV"].NewRow();
                dsCSV.Tables["TarifasCSV"].Rows.Add(new object[] { csv });
                return dsCSV;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_TarifaVentaProducto_Dataport()", exception.Message);
            }
        }

        public DataSet Sincronizar_PrimaProducto_Dataport()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_PrimaProducto_Dataport()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                DataSet dsSincronizarPrimaProducto = new DataSet();
                DataSet dsCSV = new DataSet();
                dsSincronizarPrimaProducto = this.Sincronizar_PrimaProducto();
                string csv = this.DatasetToCSV(dsSincronizarPrimaProducto, ';');
                dsCSV.Tables.Clear();
                dsCSV.Tables.Add("PrimaProductoCSV");
                dsCSV.Tables[0].Columns.Add("CSVValue");
                DataRow dr = dsCSV.Tables["PrimaProductoCSV"].NewRow();
                dsCSV.Tables["PrimaProductoCSV"].Rows.Add(new object[] { csv });
                return dsCSV;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_PrimaProducto_Dataport()", ex.Message);
            }
        }

        public DataSet Sincronizar_ComisionVendedor_Dataport()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_ComisionVendedor_Dataport()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                DataSet dsSincronizarComisionVendedor = new DataSet();
                DataSet dsCSV = new DataSet();
                dsSincronizarComisionVendedor = this.Sincronizar_ComisionVendedor();
                string csv = this.DatasetToCSV(dsSincronizarComisionVendedor, ';');
                dsCSV.Tables.Clear();
                dsCSV.Tables.Add("ComisionVendedorCSV");
                dsCSV.Tables[0].Columns.Add("CSVValue");
                DataRow dr = dsCSV.Tables["ComisionVendedorCSV"].NewRow();
                dsCSV.Tables["ComisionVendedorCSV"].Rows.Add(new object[] { csv });
                return dsCSV;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ComisionVendedor_Dataport()", ex.Message);
            }
        }


        public DataSet Sincronizar_Tiendas_Dataport()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_Tiendas_Dataport()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                DataSet dsSincronizarTiendas = new DataSet();
                DataSet dsCSV = new DataSet();
                dsSincronizarTiendas = this.Sincronizar_Tiendas();
                string csv = this.DatasetToCSV(dsSincronizarTiendas, ';');
                dsCSV.Tables.Clear();
                dsCSV.Tables.Add("TiendasCSV");
                dsCSV.Tables[0].Columns.Add("CSVValue");
                DataRow dr = dsCSV.Tables["TiendasCSV"].NewRow();
                dsCSV.Tables["TiendasCSV"].Rows.Add(new object[] { csv });
                return dsCSV;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Tiendas_Dataport()", ex.Message);
            }
        }


        public DataSet Sincronizar_Productos_Dataport(string FechaD, string FechaH)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_Productos_Dataport()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                DataSet dsSincronizarProd = new DataSet();
                DataSet dsCSV = new DataSet();
                dsSincronizarProd = this.Sincronizar_Productos(FechaD, FechaH);
                string csv = this.DatasetToCSV(dsSincronizarProd, ';');
                dsCSV.Tables.Clear();
                dsCSV.Tables.Add("ProductosCSV");
                dsCSV.Tables[0].Columns.Add("CSVValue");
                DataRow dr = dsCSV.Tables["ProductosCSV"].NewRow();
                dsCSV.Tables["ProductosCSV"].Rows.Add(new object[] { csv });
                return dsCSV;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Productos_Dataport()", ex.Message);
            }
        }

        public DataSet Sincronizar_Variantes_Dataport(string FechaD, string FechaH)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_Variantes_Dataport()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                DataSet dsSincronizarVar = new DataSet();
                DataSet dsCSV = new DataSet();
                dsSincronizarVar = this.Sincronizar_Variantes(FechaD, FechaH);
                string csv = this.DatasetToCSV(dsSincronizarVar, ';');
                dsCSV.Tables.Clear();
                dsCSV.Tables.Add("VariantesCSV");
                dsCSV.Tables[0].Columns.Add("CSVValue");
                DataRow dr = dsCSV.Tables["VariantesCSV"].NewRow();
                dsCSV.Tables["VariantesCSV"].Rows.Add(new object[] { csv });
                return dsCSV;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Variantes_Dataport()", ex.Message);
            }
        }


        public DataSet Obtener_Factura(string NumFactura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Factura()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 112; //Histórico cab. factura venta
                dt.AddColumn(2);    // Venta a-Nº cliente                                    
                dt.AddColumn(5);    // Fact. a-Nombre

                dt.AddColumn(7);    // Fact. direccion                                    
                dt.AddColumn(9);    // fact. poblacion                                 
                dt.AddColumn(13);    // envio a nombre                                     
                dt.AddColumn(15);   // envio a direccion                                    
                dt.AddColumn(17);    // envio a poblacion                                     
                //dt.AddColumn(13);    // Cód. dto. cantidad venta                                    
                dt.AddColumn(31);  // grupo contable cliente                                   
                dt.AddColumn(35);   // Precios IVA incluido                                     

                dt.AddColumn(43);   // vendedor                                     
                dt.AddColumn(70);   // CIF/NIF
                dt.AddColumn(74);   // Grupo contable negocio
                dt.AddColumn(91);     // Envío a-C.P.
                dt.AddColumn(92);     // Envío a-Provincia
                dt.AddColumn(116);     // Grupo registro IVA neg.
                dt.AddColumn(50118);     // <Crear nota de entrega>
                dt.AddColumn(88);       //codigo postal
                //
                dt.AddColumn(50111);        //Aplicar dto. empleado
                dt.AddColumn(50112);        //Nº tarjeta empleado
                dt.AddColumn(50115);        //Grupo dtos. colectivos 
                dt.AddColumn(50122);        //Descripción colectivos
                dt.AddColumn(23);           //Cód. términos pago
                dt.AddColumn(104);          //Cód. forma pago


                dt.AddFilter("No.", NumFactura);
                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 113;                         //Hist. lin factura venta
                dt.AddColumn(2);                           //Producto
                dt.AddColumn(4);                           //Nº línea
                dt.AddColumn(5);                            //tipo
                dt.AddColumn(6);                            //nº producto
                dt.AddColumn(11);                            //Descripcion
                dt.AddColumn(15);                           //Cantidad
                dt.AddColumn(22);                            //Descripcion
                dt.AddColumn(23);                            //Coste unitario (DL)
                dt.AddColumn(27);                            //% Descuento línea
                dt.AddColumn(30);                            //Importe iva incluido
                dt.AddColumn(50027);                            //ID. Pack
                dt.AddColumn(50028);                            //ID. secuencial pack
                dt.AddColumn(50029);                            //% Descuento colectivo
                dt.AddColumn(50030);                            //Importe dto. colectivo
                dt.AddColumn(51001);                            //Cantidad devuelta
                dt.AddColumn(59001);                            //Nº reserva
                dt.AddColumn(59002);                            //Crear nota de entrega
                dt.AddColumn(13);                                 //unidad de medida
                dt.AddColumn(50019);                        //Nº cheque regalo
                dt.AddColumn(5404);                     //Cdad. por unidad medida
                dt.AddColumn(5407);                     //Cód. unidad medida

                dt.AddColumn(8);                        //Grupo contable
                dt.AddColumn(74);                       //Grupo contable negocio
                dt.AddColumn(75);                       //Grupo contable producto
                dt.AddColumn(77);                       //Tipo cálculo IVA
                dt.AddColumn(89);                       //Grupo registro IVA neg.
                dt.AddColumn(90);                       //Grupo registro IVA prod.
                dt.AddColumn(25);                       //% IVA
                dt.AddColumn(5402);                     //DPA++ Variant Code
                dt.AddColumn(50040);                    //DPA++ Salesperson Code
                dt.AddFilter(5, "Item|Resource");
                da.AddTable(dt);
                da.AddRelation("Sales Invoice Header", "No.", "Sales Invoice Line", "Document No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Factura()", ex.Message);
            }
        }

        public DataSet Obtener_Reserva(string NumFactura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Reserva()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 36; //Cab. venta
                dt.AddColumn(2);    // Venta a-Nº cliente                                    
                dt.AddColumn(5);    // Fact. a-Nombre
                dt.AddColumn(7);    // Fact. direccion                                    
                dt.AddColumn(9);    // fact. poblacion                                 
                dt.AddColumn(13);    // envio a nombre                                     
                dt.AddColumn(14);
                dt.AddColumn(15);   // envio a direccion                                    
                dt.AddColumn(17);    // envio a poblacion                                     
                dt.AddColumn(18);
                dt.AddColumn(19);
                dt.AddColumn(20);
                dt.AddColumn(21);
                dt.AddColumn(23);
                dt.AddColumn(28);
                dt.AddColumn(31);  // grupo contable cliente                                   
                dt.AddColumn(34);   //grupo precio
                dt.AddColumn(35);   // Precios IVA incluido                                     
                dt.AddColumn(43);   // vendedor                                     
                dt.AddColumn(70);   // CIF/NIF
                dt.AddColumn(74);   // Grupo contable negocio
                dt.AddColumn(91);     // Envío a-C.P.
                dt.AddColumn(92);     // Envío a-Provincia
                dt.AddColumn(99);       //fecha emision documento
                dt.AddColumn(104);      //forma pago
                dt.AddColumn(116);     // Grupo registro IVA neg.
                dt.AddColumn(5700);
                dt.AddColumn(50101);
                dt.AddColumn(50102);
                dt.AddColumn(50103);
                dt.AddColumn(50104);
                dt.AddColumn(50105);
                dt.AddColumn(50106);
                dt.AddColumn(50107);
                dt.AddColumn(50108);
                dt.AddColumn(50109);
                dt.AddColumn(50110);
                dt.AddColumn(50111);
                dt.AddColumn(50112);
                dt.AddColumn(50115);
                dt.AddColumn(50118);
                dt.AddColumn(50119);
                dt.AddColumn(50120);
                dt.AddColumn(50121);
                dt.AddColumn(50122);
                dt.AddColumn(50123);
                dt.AddColumn(50124);
                dt.AddColumn(50125);
                dt.AddColumn(50126);

                dt.AddFilter("Document Type", "Order");
                dt.AddFilter("No.", NumFactura);
                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 37;                         //Hist. lin factura venta
                dt.AddColumn(2);                           //Producto
                dt.AddColumn(4);                           //Nº línea
                dt.AddColumn(5);                            //tipo
                dt.AddColumn(6);                            //nº producto
                dt.AddColumn(7);
                dt.AddColumn(8);
                dt.AddColumn(10);
                dt.AddColumn(11);                            //Descripcion
                dt.AddColumn(13);
                dt.AddColumn(15);                           //Cantidad
                dt.AddColumn(22);                            //Descripcion
                dt.AddColumn(23);                            //Coste unitario (DL)
                dt.AddColumn(25);
                dt.AddColumn(27);                            //% Descuento línea
                dt.AddColumn(28);
                dt.AddColumn(29);
                dt.AddColumn(30);                            //Importe iva incluido
                dt.AddColumn(42);
                dt.AddColumn(74);
                dt.AddColumn(75);
                dt.AddColumn(77);
                dt.AddColumn(89);
                dt.AddColumn(90);
                dt.AddColumn(100);
                dt.AddColumn(5700);
                dt.AddColumn(5709);
                dt.AddColumn(5712);
                dt.AddColumn(50019);
                dt.AddColumn(50020);
                dt.AddColumn(50021);
                dt.AddColumn(50027);                            //ID. Pack
                dt.AddColumn(50028);                            //ID. secuencial pack
                dt.AddColumn(50029);                            //% Descuento colectivo
                dt.AddColumn(50030);                            //Importe dto. colectivo
                dt.AddColumn(50031);
                dt.AddColumn(50032);
                dt.AddColumn(50034);

                dt.AddColumn(59000);
                dt.AddColumn(59001);                            //Nº reserva
                dt.AddColumn(59002);                //Crear nota de entrega
                dt.AddColumn(59003);
                dt.AddColumn(60000);

                dt.AddFilter("Document Type", "Order");
                da.AddTable(dt);
                da.AddRelation("Sales Header", "No.", "Sales Line", "Document No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Reserva()", ex.Message);
            }
        }



        public DataSet Obtener_HistTarifas()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_HistTarifas()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                //dt.TableNo = 50026;
                dt.TableName = "Hist. cab. cambio tarifa";

                dt.KeyInNavisionFormat = "Estado cambio";
                dt.AddFilter("Estado cambio", "Valid. administración|Valid. director");
                da.AddTable(dt);

                dt.Reset();
                //dt.TableNo = 50027;                         //Hist. lín. cambio tarifa
                dt.TableName = "Hist. lín. cambio tarifa";
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.AddColumn(3);
                dt.AddColumn(4);
                dt.AddColumn(5);
                dt.AddColumn(6);
                dt.AddColumn(7);
                //dt.AddColumn(8);                    
                dt.AddColumn(9);
                //dt.AddColumn(10);                   
                dt.AddColumn(11);
                //dt.AddColumn(12);
                dt.AddColumn(13);
                dt.AddColumn(14);
                dt.AddColumn(15);
                dt.AddColumn(16);
                //dt.AddColumn(17);                   
                dt.AddColumn(18);
                dt.AddColumn(19);
                dt.AddColumn(20);
                dt.AddColumn(21);
                dt.AddColumn(22);
                dt.AddColumn(23);
                dt.AddColumn(24);
                dt.AddColumn(26);
                dt.AddColumn(27);
                dt.AddColumn(28);
                dt.AddColumn(29);
                dt.AddColumn(35);
                dt.AddColumn(36);
                dt.AddColumn(37);
                dt.AddColumn(38);
                dt.AddColumn(39);
                dt.AddColumn(40);
                dt.AddColumn(41);

                da.AddTable(dt);
                da.AddRelation("Hist. cab. cambio tarifa", "Código", "Hist. lín. cambio tarifa", "Código", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_HistTarifas()", ex.Message);
            }
        }


        public DataSet Obtener_NotaEntrega(string NumNotaEntrega)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_NotaEntrega()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                //dt.TableNo = 50017;  //Cab. notas entregas
                dt.TableName = "Cab. notas entregas";
                dt.AddColumn(2);    // Venta a-Nº cliente                                    
                dt.AddColumn(5);    // Fact. a-Nombre

                dt.AddColumn(7);    // Fact. direccion                                    
                dt.AddColumn(9);    // fact. poblacion                                 
                dt.AddColumn(13);    // envio a nombre                                     
                dt.AddColumn(15);   // envio a direccion                                    
                dt.AddColumn(17);    // envio a poblacion                                     
                //dt.AddColumn(13);    // Cód. dto. cantidad venta                                    

                dt.AddColumn(91);     // Envío a-C.P.
                dt.AddColumn(92);     // Envío a-Provincia
                dt.AddColumn(50119);     // Cobrar por transportista
                dt.AddColumn(50120);     // Fact. automática
                dt.AddColumn(50121);     // Reposición inmediata
                dt.AddColumn(50123);     // Observaciones envío
                dt.AddColumn(50124);     // Importe a cobrar en entregas
                dt.AddColumn(50126);     // Importe a liq. de anticipo
                dt.AddColumn(51001);     // Cobrado por la carga
                dt.AddColumn(52000);     // Servir totalmente nota entrega
                dt.AddColumn(52001);     // estado
                dt.AddColumn(52002);     // Imp. recibido
                dt.AddColumn(52003);     // imp. total recibido

                dt.AddFilter("No.", NumNotaEntrega);
                da.AddTable(dt);

                dt.Reset();
                //dt.TableNo = 50018;                         //Lín. notas entregas
                dt.TableName = "Lín. notas entregas";
                dt.AddColumn(2);                           //Producto
                dt.AddColumn(4);                           //Nº línea
                dt.AddColumn(5);                            //tipo
                dt.AddColumn(6);                            //nº producto
                dt.AddColumn(11);                            //Descripcion
                dt.AddColumn(15);                           //Cantidad
                dt.AddColumn(22);                            //Descripcion
                dt.AddColumn(23);                            //Coste unitario (DL)
                dt.AddColumn(27);                            //% Descuento línea
                dt.AddColumn(30);                            //Importe iva incluido
                dt.AddColumn(50027);                            //ID. Pack
                dt.AddColumn(50028);                            //ID. secuencial pack
                dt.AddColumn(51001);                            //Cantidad devuelta
                dt.AddColumn(52000);                            //estado linea
                dt.AddColumn(55000);                            //Cdad. entregada
                dt.AddColumn(59001);                            //Nº reserva
                dt.AddColumn(59003);                            //Cantidad en nota entrega
                dt.AddColumn(13);                               //unidad de medida

                //dt.AddFilter(5, "Item|Resource");
                da.AddTable(dt);
                da.AddRelation("Cab. notas entregas", "No.", "Lín. notas entregas", "Document No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_NotaEntrega()", ex.Message);
            }
        }

        public DataSet Obt_EntregaCarga(string FechaDesde, string FechaHasta)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obt_EntregaCarga()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                // dt.TableNo = 50017;  //Cab. notas entregas
                dt.TableName = "Cab. notas entregas";
                dt.AddColumn(2);    // Venta a-Nº cliente    
                dt.AddColumn(3);    // nº nota entrega            
                dt.AddColumn(13);
                dt.AddColumn(14);
                dt.AddColumn(15);
                dt.AddColumn(17);
                dt.AddColumn(18);
                dt.AddColumn(20);
                dt.AddColumn(21);
                dt.AddColumn(43);
                dt.AddColumn(91);
                dt.AddColumn(92);
                dt.AddColumn(50105);
                dt.AddColumn(50107);
                dt.AddColumn(50111);
                dt.AddColumn(50112);
                dt.AddColumn(50115);
                dt.AddColumn(50118);
                dt.AddColumn(50119);
                dt.AddColumn(50120);
                dt.AddColumn(50121);
                dt.AddColumn(50123);
                dt.AddColumn(50124);
                dt.AddColumn(50126);
                dt.AddColumn(52000);
                dt.AddColumn(52001);
                dt.AddColumn(52003);

                //dt.AddFilter("No.", NumNotaEntrega);
                dt.KeyInNavisionFormat = "Posting Date";
                dt.AddFilter("Posting Date", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));
                da.AddTable(dt);

                dt.Reset();
                //dt.TableNo = 50018;                         //Lín. notas entregas
                dt.TableName = "Lín. notas entregas";
                dt.AddColumn(2);                           //cliente
                dt.AddColumn(3);                           //Nº nota entrega
                dt.AddColumn(4);
                dt.AddColumn(5);
                dt.AddColumn(6);
                dt.AddColumn(7);
                dt.AddColumn(10);
                dt.AddColumn(11);
                dt.AddColumn(13);
                dt.AddColumn(15);
                dt.AddColumn(22);
                dt.AddColumn(23);
                dt.AddColumn(25);
                dt.AddColumn(27);
                dt.AddColumn(28);
                dt.AddColumn(30);
                dt.AddColumn(5709);
                dt.AddColumn(5712);
                dt.AddColumn(59001);
                dt.AddColumn(50019);
                dt.AddColumn(50020);
                dt.AddColumn(50021);
                dt.AddColumn(59002);
                dt.AddColumn(59003);
                dt.AddColumn(50027);
                dt.AddColumn(50028);
                dt.AddColumn(51001);
                dt.AddColumn(52000);
                dt.AddColumn(55000);

                //dt.AddFilter(5, "Item|Resource");
                da.AddTable(dt);
                da.AddRelation("Cab. notas entregas", "No.", "Lín. notas entregas", "Document No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obt_EntregaCarga()", ex.Message);
            }
        }

        public DataSet Obtener_Multiformas(string NumFactura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Multiformas()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                //dt.TableNo = 50003;  //Multiformas de Pago
                dt.TableName = "Multiforma de Pago";
                dt.AddColumn(1);    // Tipo Documento                                    
                dt.AddColumn(2);    // Nº Documento
                dt.AddColumn(5);    // Nº Linea                                    
                dt.AddColumn(6);    //Cod. Forma Pago                                 
                dt.AddColumn(7);    // Tipo Contrapartida                                     
                dt.AddColumn(8);   // Cuenta Contrapartida                                   
                dt.AddColumn(9);    // Importe
                dt.AddColumn(10);   //cambio
                dt.AddColumn(12);    // Nº vale                                    
                dt.AddColumn(13);    // Tipo pago                                    
                dt.AddColumn(16);    //Cobrado                                  
                dt.AddColumn(20);    // Fecha registro                                    
                dt.AddColumn(21);    // Nº vale de reserva                             
                dt.AddColumn(22);    // Tipo de vale                                    
                dt.AddColumn(23);    // Nº cliente pago                                    
                dt.AddColumn(24);    // Nº cheque regalo                                 
                dt.AddColumn(25);    // Nº tarjeta empleado                                
                dt.AddColumn(26);    // Financiera
                dt.AddColumn(27);    // Nº autorización financiera
                dt.AddColumn(28);    // Gastos financiación
                //dt.AddColumn(29);    // % Gastos financiación
                dt.AddColumn(30);    // Cliente que financia
                dt.AddColumn(31);    // Forma pago gastos financieros
                dt.AddColumn(32);    // Cta. contrapartida gts. finan.
                dt.AddColumn(33);    // Nº tarjeta de cobro


                dt.AddFilter("Nº Documento", NumFactura);
                da.AddTable(dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Multiformas()", ex.Message);
            }
        }

        public string ObtenerFechaJefeSeccion(string Tienda)
        {
            try
            {
                if ((DBUser == null) || (DBUser.UserId == ""))
                    return "";

                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader rd;

                dt.TableNo = 60058; //Responsable_grupo_venta
                dt.AddColumn("Fecha");
                dt.AddFilter("Tienda", Tienda);
                dt.Reverse = true;
                Cmd.Table = dt;
                rd = Cmd.ExecuteReader(false);

                if (rd.RecordsAffected > 0)
                    return rd.GetDataTime(0).ToShortDateString();

                return "";

            }
            catch (Exception ex)
            {
                Utilidades.GenerarError(this.DBUser.UserCode, "ObtenerFechaJefeSeccion()", ex.Message);
                return "";
            }
        }

        public DataSet ObtenerFechaUltApertura(string Tienda)
        {
            try
            {
                if ((DBUser == null) || (DBUser.UserId == ""))
                    return Utilidades.GenerarError("", "ObtenerFechaUltApertura()", "ERROR: No se ha validado, debe abrir login");

                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader rd;

                //dt.TableNo = 50022; //Histórico aperturas tienda
                dt.TableName = "Histórico aperturas tienda";
                dt.AddColumn("Fecha apertura");
                dt.AddFilter("Cód. tienda", Tienda);
                dt.Reverse = true;
                Cmd.Table = dt;
                rd = Cmd.ExecuteReader(false);

                string result = "";

                if (rd.RecordsAffected > 0)
                    result = rd.GetDataTime(0).ToShortDateString();

                return Utilidades.GenerarResultado(result);

            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "ObtenerFechaUltApertura()", ex.Message);

            }
        }

        public DataSet Obtener_JefesSeccion(string Tienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_JefesSeccion()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                string filtroUltFecha = ObtenerFechaJefeSeccion(Tienda);

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 60058;  //Responsable_grupo_venta
                dt.AddFilter("Tienda", Tienda);
                dt.AddFilter("Fecha", filtroUltFecha);
                da.AddTable(dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_JefesSeccion()", ex.Message);
            }
        }

        public DataSet Obtener_JefesSeccion2(string Tienda, string Fecha)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_JefesSeccion2()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                //string filtroUltFecha = ObtenerFechaJefeSeccion(Tienda);

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 60058;  //Responsable_grupo_venta
                dt.AddFilter("Tienda", Tienda);
                dt.AddFilter("Fecha", Fecha);
                da.AddTable(dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_JefesSeccion2()", ex.Message);
            }
        }

        public DataSet Obtener_PteFactura(string NumFactura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_PteFactura()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 21;  //Mov. cliente
                dt.AddColumn(16);    // Importe pendiente (DL)                                    

                dt.KeyInNavisionFormat = "Document No.,Document Type,Customer No.";

                dt.AddFilter("Document No.", NumFactura);
                dt.AddFilter("Document Type", "Invoice");
                da.AddTable(dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_PteFactura()", ex.Message);
            }
        }


        public DataSet Comprobar_Apertura_Tienda(string CodTienda, string FechaApertura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Comprobar_Apertura_Tienda()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                // DataSet DsRes = new DataSet();
                // MiddleWareTPVCentral.Utilidades.Abrir_Login("121", "0000", ref DsRes, this.Connection);
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                //dt.TableNo = 50022;  //Histórico aperturas tienda
                dt.TableName = "Histórico aperturas tienda";
                dt.AddColumn(8);     //Apertura                                    

                //dt.KeyInNavisionFormat = "Document No.,Document Type,Customer No.";

                dt.AddFilter("Cód. tienda", CodTienda);
                dt.AddFilter("Fecha apertura", FechaApertura);
                //dt.AddFilter("Activa", "true");
                da.AddTable(dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Comprobar_Apertura_Tienda()", ex.Message);
            }
        }

        public DataSet Devolver_TPV_Activos(string CodTienda, string FechaApertura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Devolver_TPV_Activos()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                // DataSet DsRes = new DataSet();
                // MiddleWareTPVCentral.Utilidades.Abrir_Login("121", "0000", ref DsRes, this.Connection);
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                //dt.TableNo = 50023;  //Hist. aperturas Tienda/TPV dia
                dt.TableName = "Hist. aperturas Tienda/TPV dia";
                dt.AddColumn(1);      //Cod. tienda
                dt.AddColumn(2);      //Cod. tpv  
                dt.AddColumn(37);     //Apertura                                    

                //dt.KeyInNavisionFormat = "Document No.,Document Type,Customer No.";

                dt.AddFilter("Cód. tienda", CodTienda);
                dt.AddFilter("Fecha inicio día", FechaApertura);
                //dt.AddFilter("Activa", "true");
                da.AddTable(dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Devolver_TPV_Activos()", ex.Message);
            }
        }

        public DataSet Devolver_Jefes_Seccion(string CodTienda, string FechaApertura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Devolver_Jefes_Seccion()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                // DataSet DsRes = new DataSet();
                // MiddleWareTPVCentral.Utilidades.Abrir_Login("121", "0000", ref DsRes, this.Connection);
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 60058;  //Responsable_grupo_venta
                //dt.KeyInNavisionFormat = "Document No.,Document Type,Customer No.";

                dt.AddFilter("Tienda", CodTienda);
                dt.AddFilter("Fecha", FechaApertura);
                //dt.AddFilter("Activa", "true");
                da.AddTable(dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Devolver_Jefes_Seccion()", ex.Message);
            }
        }

        public DataSet Devolver_Vendedores(string CodTienda, string FechaApertura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Devolver_Vendedores()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                // DataSet DsRes = new DataSet();
                // MiddleWareTPVCentral.Utilidades.Abrir_Login("121", "0000", ref DsRes, this.Connection);
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableName = "Asignacion_vendedores";  //Asignacion_vendedores

                dt.AddFilter("Tienda", CodTienda);
                dt.AddFilter("Fecha", FechaApertura);

                da.AddTable(dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Devolver_Vendedores()", ex.Message);
            }
        }


        public DataSet Sincronizar_ProdSeccion(string FechaD, string FechaH, string FiltroSeccion)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_ProdSeccion()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 27; //Producto
                dt.AddColumn(1);    // Nº                                    
                dt.AddColumn(3);    // Descripción  

                dt.AddColumn(4);    // Descripción alias                                    
                dt.AddColumn(5);    // Descripción 2                                   
                dt.AddColumn(7);    // Clase                                     
                dt.AddColumn(8);   // Unidad de Medida Base                                    
                dt.AddColumn(11);    // Grupo contable existencias                                     
                //dt.AddColumn(13);    // Cód. dto. cantidad venta                                    
                dt.AddColumn(14);  // Cód. dto. producto/cliente                                    
                dt.AddColumn(18);   // Precio venta                                     

                dt.AddColumn(31);   // Nº proveedor                                     
                dt.AddColumn(32);   // Cód. producto proveedor                                     
                dt.AddColumn(91);   // Grupo contable producto
                dt.AddColumn(99);     // Grupo registro IVA prod.
                dt.AddColumn(60000);    //Calificacion
                dt.AddColumn(60001);    //Tipo_Etiqueta
                dt.AddColumn(60002);    //Tipo_Articulo
                dt.AddColumn(60003);    //Cod_Autor
                dt.AddColumn(60004);    //Cod_Seccion
                dt.AddColumn(50003);    //Producto por seccion

                dt.AddColumn("Last Date Modified");
                dt.AddColumn(5702);
                dt.AddColumn(5704);

                dt.KeyInNavisionFormat = "Cod_Seccion,Item Category Code,Product Group Code,Last Date Modified,Producto pack,Cód. tienda";
                dt.AddFilter("Cod_Seccion", FiltroSeccion);
                dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                dt.AddFilter("Producto pack", "false");
                dt.AddFilter("Blocked", "false");

                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 5717;                         //Referencia cruzada producto
                dt.AddColumn(1);                           //Producto
                dt.AddColumn(6);                           //Cod. barras asociado
                dt.AddColumn(7);                            //Descripcion
                dt.AddFilter(4, "Bar Code");                            //Cross-Reference Type
                da.AddTable(dt);
                da.AddRelation("Item", "No.", "Item Cross Reference", "Item No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ProdSeccion()", ex.Message);
            }
        }

        public DataSet Sincronizar_ProdSeccionFamSubfam(string FechaD, string FechaH,
                                                        string FiltroSeccion, string FiltroFamilia,
                                                        string FiltroSubfamilia)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_ProdSeccionFamSubfam()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 27; //Producto
                dt.AddColumn(1);    // Nº                                    
                dt.AddColumn(3);    // Descripción  

                dt.AddColumn(4);    // Descripción alias                                    
                dt.AddColumn(5);    // Descripción 2                                   
                dt.AddColumn(7);    // Clase                                     
                dt.AddColumn(8);   // Unidad de Medida Base                                    
                dt.AddColumn(11);    // Grupo contable existencias                                     
                //dt.AddColumn(13);    // Cód. dto. cantidad venta                                    
                dt.AddColumn(14);  // Cód. dto. producto/cliente                                    
                dt.AddColumn(18);   // Precio venta                                     

                dt.AddColumn(31);   // Nº proveedor                                     
                dt.AddColumn(32);   // Cód. producto proveedor                                     
                dt.AddColumn(91);   // Grupo contable producto
                dt.AddColumn(99);     // Grupo registro IVA prod.
                dt.AddColumn(60000);    //Calificacion
                dt.AddColumn(60001);    //Tipo_Etiqueta
                dt.AddColumn(60002);    //Tipo_Articulo
                dt.AddColumn(60003);    //Cod_Autor
                dt.AddColumn(60004);    //Cod_Seccion
                dt.AddColumn(50003);    //Producto por seccion

                dt.AddColumn("Last Date Modified");
                dt.AddColumn(5702);
                dt.AddColumn(5704);

                dt.KeyInNavisionFormat = "Cod_Seccion,Item Category Code,Product Group Code,Last Date Modified,Producto pack,Cód. tienda";
                dt.AddFilter("Cod_Seccion", FiltroSeccion);
                dt.AddFilter("Item Category Code", FiltroFamilia);
                dt.AddFilter("Product Group Code", FiltroSubfamilia);
                dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                dt.AddFilter("Producto pack", "false");
                dt.AddFilter("Blocked", "false");

                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 5717;                         //Referencia cruzada producto
                dt.AddColumn(1);                           //Producto
                dt.AddColumn(6);                           //Cod. barras asociado
                dt.AddColumn(7);                            //Descripcion
                dt.AddFilter(4, "Bar Code");                            //Cross-Reference Type
                da.AddTable(dt);
                da.AddRelation("Item", "No.", "Item Cross Reference", "Item No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ProdSeccionFamSubfam()", ex.Message);
            }
        }

        public DataSet Sincronizar_ProductosST(string FechaD, string FechaH)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_ProductosST()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 27; //Producto
                dt.AddColumn(1);    // Nº                                    
                dt.AddColumn(3);    // Descripción  

                dt.AddColumn(4);    // Descripción alias                                    
                dt.AddColumn(5);    // Descripción 2                                   
                dt.AddColumn(7);    // Clase                                     
                dt.AddColumn(8);   // Unidad de Medida Base                                    
                dt.AddColumn(11);    // Grupo contable existencias                                     
                //dt.AddColumn(13);    // Cód. dto. cantidad venta                                    
                dt.AddColumn(14);  // Cód. dto. producto/cliente                                    
                dt.AddColumn(18);   // Precio venta                                     

                dt.AddColumn(31);   // Nº proveedor                                     
                dt.AddColumn(32);   // Cód. producto proveedor                                     
                dt.AddColumn(91);   // Grupo contable producto
                dt.AddColumn(99);     // Grupo registro IVA prod.
                //dt.AddColumn(60000);    //Calificacion
                //dt.AddColumn(60001);    //Tipo_Etiqueta
                //dt.AddColumn(60002);    //Tipo_Articulo
                //dt.AddColumn(60003);    //Cod_Autor
                //dt.AddColumn(60004);    //Cod_Seccion
                dt.AddColumn(50003);    //Producto por seccion

                dt.AddColumn("Last Date Modified");
                dt.KeyInNavisionFormat = "Last Date Modified";
                dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));

                dt.AddFilter("Blocked", "false");

                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 5717;                         //Referencia cruzada producto
                dt.AddColumn(1);                           //Producto
                dt.AddColumn(6);                           //Cod. barras asociado
                dt.AddColumn(7);                            //Descripcion
                dt.AddFilter(4, "Bar Code");                            //Cross-Reference Type
                da.AddTable(dt);
                da.AddRelation("Item", "No.", "Item Cross Reference", "Item No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ProductosST()", ex.Message);
            }
        }

        public DataSet Sincronizar_Tarifa_OfGenerica(string FechaD,
                                                     string FechaH,
                                                     string CodTienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_Tarifa_OfGenerica()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 60029; // Relacion_Tarifa_Tienda
                dt.AddColumn(1);    // Cod_Tarifa                                    
                dt.AddColumn(2);    // Cod_Tienda
                dt.AddFilter("Cod_Tienda", CodTienda + "|''");

                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 7002;                         //Sales Price
                dt.AddColumn(1);                        //Nº producto
                dt.AddColumn(2);                        //Código ventas  
                dt.AddColumn(3);                        //"Cód. divisa");
                dt.AddColumn(4);                        //"Fecha inicial");
                dt.AddColumn(5);                        //"Precio venta");
                dt.AddColumn(7);                        //"Precio IVA incluido");
                dt.AddColumn(10);                       //"Permite dto. cantidad");
                dt.AddColumn(7001);                     //"Permite dto. cliente/producto");
                dt.AddColumn(11);                       //"Gr.regis. IVA negocio (precio)");
                dt.AddColumn(5400);                     //"Cód. unidad medida");
                dt.AddColumn("Last Date Modified");
                string filtroFecha = MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH);
                dt.AddFilter("Last Date Modified", filtroFecha);
                dt.KeyInNavisionFormat = "Sales Code,Last Date Modified";
                da.AddTable(dt);

                da.AddRelation("Relacion_Tarifa_Tienda", "Cod_Tarifa", "Sales Price", "Sales Code", NavisionDBAdapter.JoinType.Inner_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Tarifa_OfGenerica()", ex.Message);
            }
        }

        public DataSet Sincronizar_GruPrecio_OfGenerica(string CodTienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_GruPrecio_OfGenerica()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 60029; // Relacion_Tarifa_Tienda
                dt.AddColumn(1);    // Cod_Tarifa                                    
                dt.AddColumn(2);    // Cod_Tienda
                dt.AddFilter("Cod_Tienda", CodTienda + "|''");

                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 6;     //Customer Price Group
                dt.AddColumn(1);    // Código                                
                dt.AddColumn(2);    // Precio IVA incluido
                dt.AddColumn(5);    // Permite dto. factura                                  
                dt.AddColumn(6);    // Gr.regis. IVA negocio (precio)                                  
                dt.AddColumn(10);    // descripcion
                dt.AddColumn(7001);    // permite descuento linea
                dt.AddColumn(60000);    // Tipo_Grupo
                //string filtroFecha = MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH);
                //dt.AddFilter("Last Date Modified", filtroFecha);
                //dt.KeyInNavisionFormat = "Sales Code,Last Date Modified";

                da.AddTable(dt);
                da.AddRelation("Relacion_Tarifa_Tienda", "Cod_Tarifa", "Customer Price Group", "Code", NavisionDBAdapter.JoinType.Inner_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GruPrecio_OfGenerica()", ex.Message);
            }
        }

        public DataSet Sincronizar_PedCompra(string CodTienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_PedCompra()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 38; // Cab. compra
                dt.AddColumn(2);    // nº proveedor                                    
                dt.AddColumn(3);    // nº pedido
                dt.AddColumn(13);
                dt.AddColumn(14);
                dt.AddColumn(15);
                dt.AddColumn(17);
                dt.AddColumn(18);
                dt.AddColumn(19);
                dt.AddColumn(20);
                dt.AddColumn(21);
                dt.AddColumn(23);
                dt.AddColumn(28);
                dt.AddColumn(31);
                dt.AddColumn(35);
                dt.AddColumn(70);
                dt.AddColumn(74);
                dt.AddColumn(79);
                dt.AddColumn(80);
                dt.AddColumn(81);
                dt.AddColumn(83);
                dt.AddColumn(84);
                dt.AddColumn(88);
                dt.AddColumn(89);
                dt.AddColumn(90);
                dt.AddColumn(91);
                dt.AddColumn(92);
                dt.AddColumn(99);
                dt.AddColumn(104);
                dt.AddColumn(116);

                dt.KeyInNavisionFormat = "Location Code,Document Type";
                dt.AddFilter("Location Code", CodTienda);
                dt.AddFilter("Document Type", "Order");

                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 39;     //Lín. compra
                //dt.AddColumn(2); 
                dt.AddColumn(3);
                dt.AddColumn(4);
                dt.AddColumn(5);
                dt.AddColumn(6);
                //dt.AddColumn(7); 
                //dt.AddColumn(8);
                dt.AddColumn(10);
                dt.AddColumn(11);
                //dt.AddColumn(13);
                dt.AddColumn(16);
                dt.AddColumn(22);
                //dt.AddColumn(23);
                //dt.AddColumn(25);
                //dt.AddColumn(29);
                //dt.AddColumn(30);
                //dt.AddColumn(31);
                //dt.AddColumn(74);
                //dt.AddColumn(75);
                //dt.AddColumn(77);
                //dt.AddColumn(89);
                //dt.AddColumn(90);
                //dt.AddColumn(100);
                //dt.AddColumn(5709);
                //dt.AddColumn(5712);
                //dt.AddColumn(5795);


                //string filtroFecha = MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH);
                //dt.AddFilter("Last Date Modified", filtroFecha);
                //dt.KeyInNavisionFormat = "Sales Code,Last Date Modified";

                dt.AddFilter("Document Type", "Order");
                da.AddTable(dt);
                da.AddRelation("Purchase Header", "No.", "Purchase Line", "Document No.", NavisionDBAdapter.JoinType.Inner_Join, false);


                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_PedCompra()", ex.Message);
            }
        }


        public DataSet Obtener_Diario_Producto(string Tienda, string Seccion)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Diario_Producto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableName = "Ajustes Inventario TPV";
                dt.AddColumn("Numero");
                dt.AddColumn("Nº Documento");     //Nº documento
                dt.AddColumn("Estado");
                dt.AddColumn("Seccion");    //Seccion
                dt.AddColumn("Nº línea");    //Nº linea
                dt.AddColumn("Entry Type");    //Tipo movimiento
                dt.AddColumn("Nº producto");    //Nº producto
                dt.AddColumn("Cantidad");    //Cantidad
                dt.AddColumn("Libro");    //Libro
                dt.AddColumn("Cód. tienda");    //Cod. tienda
                dt.AddColumn("Fecha registro");    //Fecha registro
                dt.AddColumn("Cod. variante"); //Cód. variante

                dt.KeyInNavisionFormat = "Cód. tienda,Seccion,Estado";
                
                dt.AddFilter("Cód. tienda", Tienda);
                dt.AddFilter("Seccion", Seccion);
                dt.AddFilter("Estado", "Enviar a tienda");  //Enviar a tienda    
          
                da.AddTable(dt);
                da.Fill(ref ds, true);
                Utilidades.CompletarDataSet(ref ds, false, false);


                //VUELVO A REALIZAR LA CONSULTA Y LOS MODIFICO
                da = new NavisionDBAdapter();
                dt.Reset();
                dt.TableName = "Ajustes Inventario TPV";
                dt.AddColumn("Estado");
                dt.AddColumn("Fecha modificación");
                dt.AddColumn("Hora modificación");

                dt.Modify("Fecha modificación", DateTime.Now.ToString("dd/MM/yyyy"));
                dt.Modify("Hora modificación", DateTime.Now.ToString("HH:mm:ss"));
                dt.Modify("Estado", "Enviado");

                dt.KeyInNavisionFormat = "Cód. tienda,Seccion";

                dt.AddFilter("Cód. tienda", Tienda);
                dt.AddFilter("Seccion", Seccion);
                dt.AddFilter("Estado", "Enviar a tienda");

                da.AddTable(dt);

                try
                {
                    this.Connection.BWT();
                    da.Update();                    
                    this.Connection.EWT();
                }
                catch (Exception excep)
                {
                    this.Connection.AWT();
                    throw new Exception("Obtener_Diario_Producto(): " + excep.Message);
                }

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Diario_Producto()", ex.Message);
            }
        }

        public DataSet Resultado_Ajuste_Inventario(string Numero, string Tienda, string Seccion, bool Resultado, string ErrorMsg)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Resultado_Ajuste_Inventario()", "ERROR: No se ha validado, debe abrir login");
            try
            {

                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                da = new NavisionDBAdapter();

                dt.Reset();
                dt.TableName = "Ajustes Inventario TPV";
                dt.AddColumn("Estado");
                dt.AddColumn("Fecha modificación");
                dt.AddColumn("Hora modificación");
                dt.AddColumn("Error Message");
                dt.Modify("Fecha modificación", DateTime.Now.ToString("dd/MM/yyyy"));
                dt.Modify("Hora modificación", DateTime.Now.ToString("HH:mm:ss"));
                dt.Modify("Error Message", ErrorMsg);

                if (Resultado == true)
                {
                    dt.Modify("Estado", "Procesado");
                    dt.Modify("Error Message", "");
                }
                else
                {
                    dt.Modify("Estado", "Error");
                    dt.Modify("Error Message", ErrorMsg);
                }

                dt.AddFilter("Numero", Numero);
                da.AddTable(dt);

                try
                {
                    this.Connection.BWT();
                    da.Update();
                    this.Connection.EWT();
                }
                catch (Exception excep)
                {
                    this.Connection.AWT();
                    throw new Exception("Resultado_Ajuste_Inventario(): " + excep.Message);
                }

                return Utilidades.GenerarResultado("Correcto");
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Resultado_Ajuste_Inventario()", ex.Message);
            }
        }
        public DataSet PruebaDate()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Reaprovision_tienda()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2
                //dt.TableNo = 50001;  //Mensajes TPV
                dt.TableName = "Item";
                dt.AddColumn(62);     //Descripcion

                DateTime pp = new DateTime();
                pp = Convert.ToDateTime("01/08/2007");
                dt.Modify(62, pp);
                dt.AddFilter("No.", "J34");

                da.AddTable(dt);
                try
                {
                    this.Connection.BWT();
                    da.Update();
                    this.Connection.EWT();
                }
                catch (Exception excep)
                {
                    this.Connection.AWT();
                    throw new Exception("Obtener_Reaprovision_tienda(): " + excep.Message);
                }
                da.Fill(ref ds, true);

                Utilidades.CompletarDataSet(ref ds, false, false);


                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "PruebaDate()", ex.Message);
            }
        }


        public DataSet Obtener_Reaprovision_Tienda(string Tienda)
        {
            return this.Obtener_Reaprovision_Tienda(Tienda, "", true);
        }

        //DPA++
        public DataSet Obtener_Reaprovision_Tienda(string Tienda, string NoDocumento, bool delete)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Obtener_Reaprovision_tienda()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                string msqServerName = ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
                string msqsqltonavision = ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
                string msqsqlfromnavision = ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 83; //Lín. diario producto
                table.KeyInNavisionFormat = "Almacen_Destino,Tipo_Linea_TPV";
                table.AddFilter("Almacen_Destino", Tienda);
                table.AddFilter("Tipo_Linea_TPV", "Transferencia");
                if (NoDocumento != "")
                {
                    table.AddFilter("Document No.", NoDocumento);
                }
                adapter.AddTable(table);
                adapter.Fill(ref ds, true);
                Utilidades.CompletarDataSet(ref ds, false, false);
                if (delete)
                {
                    try
                    {
                        for (int i = 0; i <= (ds.Tables[0].Rows.Count - 1); i++)
                        {
                            NavisionDBNas nas = new NavisionDBNas();
                            nas.NasInitializeChannels(msqServerName, msqsqlfromnavision, msqsqltonavision);
                            nas.ExecuteFunction = "PSTD_DiarioProd_Borrar";
                            string[] strArray = new string[3];
                            string str4 = Convert.ToInt32(ds.Tables[0].Rows[i]["Line No."]).ToString();
                            strArray[0] = ds.Tables[0].Rows[i]["Journal Template Name"].ToString();
                            strArray[1] = ds.Tables[0].Rows[i]["Journal Batch Name"].ToString();
                            strArray[2] = str4;
                            nas.Parameters = strArray;
                            nas.SendParamsAsync(nas, "", false);
                        }
                    }
                    catch (System.Messaging.MessageQueueException exception)
                    {
                        return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Reaprovision_Tienda()", "Error al insertar l\x00ednea: " + exception.Message);
                    }
                }
                return ds;
            }
            catch (Exception exception2)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Reaprovision_tienda()", exception2.Message);
            }
        }
        //DPA--

        //DPA++
        public DataSet Obtener_Reaprovision_Tienda_Independiente(string Tienda)
        {
            return this.Obtener_Reaprovision_Tienda_Independiente(Tienda, "", true);
        }
        //DPA--

        //DPA++
        public DataSet Obtener_Reaprovision_Tienda_Independiente(string Tienda, string NoDocumento, bool delete)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Obtener_Reaprovision_Tienda_Independiente()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableName = "Transfer TPV Line";
                table.AddFilter("Transfer-to Code", Tienda);
                if (!string.IsNullOrEmpty(NoDocumento))
                {
                    table.AddFilter("Document No.", NoDocumento);
                }
                adapter.AddTable(table);
                adapter.Fill(ref ds, true);
                Utilidades.CompletarDataSet(ref ds, false, false);
                if (delete)
                {
                    NavisionDBTable tableToDelete = new NavisionDBTable(this.Connection, this.DBUser);
                    NavisionDBAdapter adapterToDelete = new NavisionDBAdapter();
                    DataSet dsToDelete = new DataSet();
                    tableToDelete.TableName = "Transfer TPV Line";
                    tableToDelete.AddFilter("Transfer-to Code", Tienda);
                    if (!string.IsNullOrEmpty(NoDocumento))
                    {
                        tableToDelete.AddFilter("Document No.", NoDocumento);
                    }
                    adapterToDelete.AddTable(tableToDelete);
                    adapterToDelete.Fill(ref dsToDelete, true);
                    adapterToDelete.DeleteItem = dsToDelete;
                    try
                    {
                        tableToDelete.ConnectionDB.BWT();
                        adapterToDelete.Update();
                        tableToDelete.ConnectionDB.EWT();
                    }
                    catch (Exception ex)
                    {
                        return Utilidades.GenerarError(this.DBUser.UserCode + "-" + NoDocumento, "Obtener_Reaprovision_Tienda_Independiente()", ex.Message);
                    }
                }
                return ds;
            }
            catch (Exception ex2)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode + "-" + NoDocumento, "Obtener_Reaprovision_Tienda_Independiente()", ex2.Message);
            }
        }
        //DPA--


        public DataSet Obtener_Reposicion_Tienda(string Tienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Reposicion_tienda()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableName = "Cab. reposicion";
                dt.AddColumn("Nº");
                dt.AddColumn("Almacen Origen");
                dt.AddColumn("Descripcion origen");
                dt.AddColumn("Destino");
                dt.AddColumn("Descripcion Destino");
                dt.AddColumn("Fecha envío");
                dt.AddColumn("Fecha registro");
                dt.AddColumn("Fecha emisión documento");

                dt.KeyInNavisionFormat = "Destino,Estado";
                dt.AddFilter("Destino", Tienda);
                dt.AddFilter("Estado", "Cerrada");
                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Reposicion_tienda()", ex.Message);
            }
        }

        public DataSet Obtener_Entregas_Tienda(string Tienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Entregas_tienda()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableName = "Sales Header";           //CABECERA DE PEDIDO
                dt.AddColumn(1);        //"document type"
                dt.AddColumn(2);        //Cliente
                dt.AddColumn(3);        //nº documento
                dt.AddColumn(20);       //fecha registro
                dt.AddColumn(28);       //almacen
                dt.AddColumn(12);       //cod. direccion envío
                dt.AddColumn(13);        //envio nombre
                dt.AddColumn(15);        //envio direccion
                dt.AddColumn(17);       //envio poblacion
                dt.AddColumn(18);       //envio atencion
                dt.AddColumn(91);       //envio CP
                dt.AddColumn(92);       //envio provincia
                dt.AddColumn(50102);    //importe entregado
                dt.AddColumn(5700);     //Centro responsabilidad = tienda origen

                dt.KeyInNavisionFormat =
                    "Document Type,Location Code,Entrega de otra tienda,Enviada a tienda destino,Reg. en tienda destino";
                dt.AddFilter("Document Type", "Order");
                dt.AddFilter("Location Code", Tienda);
                dt.AddFilter("Entrega de otra tienda", "true");
                dt.AddFilter("Enviada a tienda destino", "false");
                da.AddTable(dt);

                dt.Reset();
                dt.TableNo = 37;            //  LINEAS DE PEDIDO
                dt.AddColumn(1);            //"document type"  
                dt.AddColumn(3);            //"nº documento"
                dt.AddColumn(4);            //nº linea
                dt.AddColumn(6);            //nº = producto  
                dt.AddColumn(11);            //descripcion
                dt.AddColumn(15);            //cantidad
                dt.AddColumn(22);            //precio
                dt.AddColumn(27);            //%dto. linea

                dt.AddFilter(1, "Order");                            //Cross-Reference Type
                da.AddTable(dt);
                da.AddRelation("Sales Header", "No.", "Sales Line", "Document No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001


                //VUELVO A REALIZAR LA CONSULTA PARA MODIFICAR EL CAMPO
                //ENVIADA A TIENDA DESTINO = TRUE, PARA QUE NO VUELVA EN LA SIGUIENTE CONSULTA
                //DE ENTREGAS DE DISTINTAS TIENDAS
                dt.Reset();
                da = new NavisionDBAdapter();
                DataSet ds2 = new DataSet();
                dt.TableName = "Sales Header";
                //dt.Modify("Enviada a tienda destino", "true");

                dt.KeyInNavisionFormat =
                    "Document Type,Location Code,Entrega de otra tienda,Enviada a tienda destino,Reg. en tienda destino";
                dt.AddFilter("Document Type", "Order");
                dt.AddFilter("Location Code", Tienda);
                dt.AddFilter("Entrega de otra tienda", "true");
                dt.AddFilter("Enviada a tienda destino", "false");
                da.AddTable(dt);
                da.Fill(ref ds2, true);
                for (int i = 0; i < ds2.Tables[0].Rows.Count; i++)
                {
                    ds2.Tables[0].Rows[i]["Enviada a tienda destino"] = true;
                }
                ds2.Tables[0].AcceptChanges();
                da.UpdateItem = ds2;

                try
                {
                    dt.ConnectionDB.BWT();
                    da.Update();
                    dt.ConnectionDB.EWT();
                }
                catch
                {
                    dt.ConnectionDB.AWT();
                }

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Entregas_tienda()", ex.Message);
            }
        }

        public DataSet Obtener_Entregas_Estado(string Tienda)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Entregas_Estado()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableName = "Sales Header";           //CABECERA DE PEDIDO
                dt.AddColumn(3);        //nº documento
                dt.AddColumn(50103);    //Estado reserva

                dt.KeyInNavisionFormat =
                    "Document Type,Cód. tienda,Entrega de otra tienda,Enviada a tienda destino,Reg. en tienda destino";
                dt.AddFilter("Document Type", "Order");
                dt.AddFilter("Cód. tienda", Tienda);
                dt.AddFilter("Entrega de otra tienda", "true");
                dt.AddFilter("Enviada a tienda destino", "true");
                dt.AddFilter("Reg. en tienda destino", "false");
                dt.AddFilter("Estado Reserva", "Facturada|Anulada");
                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001


                //VUELVO A REALIZAR LA CONSULTA PARA MODIFICAR EL CAMPO
                //Reg. en tienda destino = TRUE, PARA QUE NO VUELVA EN LA SIGUIENTE CONSULTA
                //DE ENTREGAS DE DISTINTAS TIENDAS
                dt.Reset();
                da = new NavisionDBAdapter();
                DataSet ds2 = new DataSet();
                dt.TableName = "Sales Header";
                //dt.Modify("Reg. en tienda destino", "true");

                dt.KeyInNavisionFormat =
                    "Document Type,Cód. tienda,Entrega de otra tienda,Enviada a tienda destino,Reg. en tienda destino";
                dt.AddFilter("Document Type", "Order");
                dt.AddFilter("Cód. tienda", Tienda);
                dt.AddFilter("Entrega de otra tienda", "true");
                dt.AddFilter("Enviada a tienda destino", "true");
                dt.AddFilter("Reg. en tienda destino", "false");
                dt.AddFilter("Estado Reserva", "Facturada|Anulada");
                da.AddTable(dt);
                da.Fill(ref ds2, true);
                for (int i = 0; i < ds2.Tables[0].Rows.Count; i++)
                {
                    ds2.Tables[0].Rows[i]["Reg. en tienda destino"] = true;
                }
                ds2.Tables[0].AcceptChanges();
                da.UpdateItem = ds2;
                try
                {
                    dt.ConnectionDB.BWT();
                    da.Update();
                    dt.ConnectionDB.EWT();
                }
                catch
                {
                    dt.ConnectionDB.AWT();
                }

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Entregas_Estado()", ex.Message);
            }
        }

        public DataSet Obtener_Reposicion_Tienda_Lineas(string Numero)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Obtener_Reposicion_Tienda_Lineas()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableName = "Lín. reposicion";
                dt.AddColumn("Nº documento");
                dt.AddColumn("Nº línea");
                dt.AddColumn("Nº");
                dt.AddColumn("Descripción");
                dt.AddColumn("Cantidad");
                dt.AddColumn("Cantidad recibida");

                dt.AddFilter("Nº documento", Numero);

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Obtener_Reposicion_Tienda_Lineas()", ex.Message);
            }
        }
        public DataSet Actualizar_lineas_Reposicion(string Documento, string Linea, string CantidadRecibida)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Actualizar_lineas_Reposicion()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                Int32 K;
                K = Convert.ToInt32(CantidadRecibida);
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableName = "Lín. reposicion";
                Dt.AddColumn("Nº documento");
                Dt.AddColumn("Nº línea");

                Dt.AddColumn("Cantidad recibida");

                Dt.Modify("Cantidad recibida", K);

                Dt.AddFilter("Nº documento", Documento);
                Dt.AddFilter("Nº línea", Linea);

                da.AddTable(Dt);
                try
                {
                    Dt.ConnectionDB.BWT();
                    da.Update();
                    Dt.ConnectionDB.EWT();
                }
                catch
                {
                    Dt.ConnectionDB.AWT();
                }

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Actualizar_lineas_Reposicion(): ", ex.Message);
            }
        }
        public DataSet Sincronizar_GrupoPrecioCliente(string CodTarifa)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_GrupoPrecioCliente()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 6;      // Tabla Tarifa en 2.6
                dt.AddColumn(1);    // Código                                
                dt.AddColumn(2);    // Precio IVA incluido

                //dt.AddColumn(3);    // Permite dto. cantidad                                  
                //dt.AddColumn(4);    // Permite dto. cliente/producto                                
                dt.AddColumn(5);    // Permite dto. factura                                  
                dt.AddColumn(6);    // Gr.regis. IVA negocio (precio)                                  
                dt.AddColumn(10);    // descripcion
                dt.AddColumn(7001);    // permite descuento linea
                dt.AddFilter("Code", CodTarifa);

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_GrupoPrecioCliente()", ex.Message);
            }
        }

        public DataSet Sincronizar_PrimaProducto()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_PrimaProducto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 50042;      // Primas productos


                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_PrimaProducto()", ex.Message);
            }
        }

        public DataSet Sincronizar_ComisionVendedor()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_ComisionVend()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 50036;      // Tabla Tarifa en 2.6


                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_ComisionVend()", ex.Message);
            }
        }

        public DataSet Sincronizar_HistLinFactura(string NumFactura)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_HistLinFactura()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 113;      // Histórico lín. factura venta
                dt.AddColumn(2);        //Venta a-Nº cliente
                dt.AddColumn(3);    // Nº documento                                
                dt.AddColumn(4);    // Nº línea

                dt.AddColumn(6);    // Nº                                  
                dt.AddColumn(7);    // Cód. almacén                                
                dt.AddColumn(11);    // Descripción                                  
                dt.AddColumn(15);    // Cantidad
                dt.AddColumn(22);    // Precio venta
                dt.AddColumn(30);    // Importe IVA incl.
                dt.AddColumn(50019);    //Nº cheque regalo
                dt.AddFilter("Document No.", NumFactura);
                dt.AddFilter("Type", "Item");

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_HistLinFactura()", ex.Message);
            }
        }

        public DataSet Sincronizar_TarifaVentaProducto(string FechaD,
                                                       string FechaH,
                                                       string CodTarifa)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_TarifaVentaProducto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2
                // OJO esta tabla no existe en Navision 4.0 SP2

                //dt.TableName = "Tarifa venta producto";
                dt.TableNo = 7002;                      //Precio venta
                dt.AddColumn(1);                        //Nº producto
                dt.AddColumn(2);                        //Código ventas  
                dt.AddColumn(3);                        //"Cód. divisa");
                dt.AddColumn(4);                        //"Fecha inicial");
                dt.AddColumn(5);                        //"Precio venta");
                dt.AddColumn(7);                        //"Precio IVA incluido");
                dt.AddColumn(10);                       //"Permite dto. cantidad");
                dt.AddColumn(7001);                     //"Permite dto. cliente/producto");
                dt.AddColumn(11);                       //"Gr.regis. IVA negocio (precio)");
                dt.AddColumn(5400);                     //"Cód. unidad medida");
                dt.AddColumn(5700);                     //"Variant Code"
                //dt.AddColumn("Descripcion Producto");
                //dt.AddColumn("CodProdProvee");
                dt.AddColumn("Last Date Modified");
                dt.KeyInNavisionFormat = "Sales Code,Last Date Modified";
                dt.AddFilter("Sales Code", CodTarifa);
                dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaD) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaH));
                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_TarifaVentaProducto()", ex.Message);
            }
        }

        public DataSet Sincronizar_TarifaVentaProducto(string FechaD, string FechaH, string CodTarifa, string PrecioIni)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_TarifaVentaProducto()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 7002; //Precio venta
                table.AddColumn(1);
                table.AddColumn(2);
                table.AddColumn(3);
                table.AddColumn(4);
                table.AddColumn(5);
                table.AddColumn(7);

                //>>jdp añadido 20/05/2010
                table.AddColumn(10);
                table.AddColumn(13);
                table.AddColumn(7001);
                table.AddColumn(11);
                //<<jdp añadido 20/05/2010

                /* //>>ICP.EB_20100511
                table.AddColumn(16); //NO EXISTE COLUMNA CON Nº 16 ?!
                table.AddColumn(19);
                table.AddColumn(17);
                //<<ICP.EB_20100511 */

                table.AddColumn(5400);
                table.AddColumn(5700);
                table.AddColumn("Last Date Modified");
                table.AddColumn("PrecioIniciTPV");
                table.AddColumn("Ending Date");
                table.KeyInNavisionFormat = "PrecioIniciTPV,Last Date Modified";
                //>>EB.ICP.20100531
                //table.AddFilter("PrecioIniciTPV", PrecioIni);
                //<<EB.ICP.20100531
                table.AddFilter("Last Date Modified", Utilidades.FechaDesde(FechaD) + ".." + Utilidades.FechaHasta(FechaH));
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                try
                {
                    this.Connection.close();
                    this.Connection.Dispose();
                    this.Connection = Utilidades.Conectar_Navision();
                }
                catch (Exception ex2)
                {
                    Console.WriteLine(ex2.Message);
                }
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_TarifaVentaProducto()", exception.Message);
            }
        }






        public DataSet Sincronizar_TarifasProdPrecioIni(string FechaDesde,
                                                        string FechaHasta,
                                                        string PrecioIni)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_TarifasProdPrecioIni()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();


                dt.TableNo = 7002;                      //Precio venta
                dt.AddColumn(1);                        //Nº producto
                dt.AddColumn(2);                        //Código ventas  
                dt.AddColumn(3);                        //"Cód. divisa");
                dt.AddColumn(4);                        //"Fecha inicial");
                dt.AddColumn(5);                        //"Precio venta");
                dt.AddColumn(7);                        //"Precio IVA incluido");
                dt.AddColumn(10);                       //"Permite dto. cantidad");
                dt.AddColumn(7001);                     //"Permite dto. cliente/producto");
                dt.AddColumn(11);                       //"Gr.regis. IVA negocio (precio)");
                dt.AddColumn(5400);                     //"Cód. unidad medida");
                dt.AddColumn(5700);                     //"Variant Code"
                dt.AddColumn("PrecioIniciTPV");          //Precio Inicio

                dt.AddColumn("Last Date Modified");
                dt.KeyInNavisionFormat = "PrecioIniciTPV,Last Date Modified";
                //EB.ICP.20100531
                //dt.AddFilter("PrecioIniciTPV", PrecioIni);
                //EB.ICP.20100531
                dt.AddFilter("Last Date Modified", MiddleWareTPVCentral.Utilidades.FechaDesde(FechaDesde) + ".." + MiddleWareTPVCentral.Utilidades.FechaHasta(FechaHasta));
                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_TarifaVentaProducto()", ex.Message);
            }
        }



        public DataSet Sincronizar_CargoProducto(string CodCargo)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_CargoProducto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2
                // OJO esta tabla no existe en Navision 4.0 SP2

                //dt.TableName = "Item Charge";
                dt.TableNo = 5800;                     //Cargo prod.                
                dt.AddFilter("No.", CodCargo);
                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_CargoProducto()", ex.Message);
            }
        }

        public DataSet Sincronizar_Tiendas()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Sincronizar_Tiendas()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableNo = 14;  // Location
                dt.AddColumn(1);  // Code
                dt.AddColumn(2);  // Name
                dt.AddColumn("Tipo Almacen");
                dt.AddColumn(50081);
                dt.AddColumn(50082);
                dt.AddColumn(50083);
                dt.AddColumn(50084);
                dt.AddColumn("Address");
                dt.AddColumn("Address 2");
                dt.AddColumn("City");
                dt.AddColumn("Post Code");
                dt.AddColumn("County");
                dt.AddColumn("Country/Region Code");
                dt.AddColumn("Phone No.");
                dt.AddColumn("E-Mail");
                dt.AddColumn("Use As In-Transit");

                dt.KeyInNavisionFormat = "Tipo Almacen";
                //EC-AQ: 22-08-2008: Añadir al filtro franquicie para que desde la tpv se puedan sincronizar almacenes de 
                //tipo franquicie.
                dt.AddFilter("Tipo Almacen", "Tienda|Central|Franquicia|Web");
                //EC-AQ: 22-08-2008: Fin
                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Tiendas()", ex.Message);
            }
        }

        //DPA++
        public DataSet Sincronizar_Tickets_Con_Integridad(string _codTienda, DataSet TicketsDia)
        {
            DataSet set = new DataSet();
            NavisionDBAdapter adapterSet = new NavisionDBAdapter();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_Tickets_Con_Integridad()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                this.DeleteOldOustandingTickets(_codTienda);
                if ((TicketsDia.Tables.Count > 0) && (TicketsDia.Tables[0].Rows.Count > 0))
                {
                    for (int num3 = 0; num3 <= (TicketsDia.Tables[0].Rows.Count - 1); num3++)
                    {
                        string tipoDocumento = TicketsDia.Tables[0].Rows[num3][0].ToString();
                        string documentNo = TicketsDia.Tables[0].Rows[num3][1].ToString();
                        string codTienda = TicketsDia.Tables[0].Rows[num3][2].ToString();
                        string codTPV = TicketsDia.Tables[0].Rows[num3][3].ToString();
                        string fechaDocumento = TicketsDia.Tables[0].Rows[num3][4].ToString();
                        string errorEnvio = TicketsDia.Tables[0].Rows[num3][5].ToString();
                        int numLineas = Convert.ToInt32(TicketsDia.Tables[0].Rows[num3][6].ToString());
                        switch (tipoDocumento)
                        {
                            case "Factura":
                                if (!this.IsInvoiceInCentralOK(documentNo, numLineas))
                                {
                                    if (errorEnvio == "No")
                                    {
                                        this.InsertTicketPendiente(tipoDocumento, documentNo, codTienda, codTPV, fechaDocumento, 1);
                                    }
                                }
                                else if (errorEnvio == "Sí")
                                {
                                    this.InsertTicketPendiente(tipoDocumento, documentNo, codTienda, codTPV, fechaDocumento, 0);
                                }
                                break;

                            case "Devolución":
                                if (!this.IsCrMemoInCentralOK(documentNo, numLineas))
                                {
                                    if (errorEnvio == "No")
                                    {
                                        this.InsertTicketPendiente(tipoDocumento, documentNo, codTienda, codTPV, fechaDocumento, 1);
                                    }
                                }
                                else if (errorEnvio == "Sí")
                                {
                                    this.InsertTicketPendiente(tipoDocumento, documentNo, codTienda, codTPV, fechaDocumento, 0);
                                }
                                break;
                        }
                    }
                    set = this.GetTicketsPendientes(_codTienda);
                }
                else
                {
                    set = Utilidades.GenerarResultado("Nada que hacer.");
                }
            }
            catch (Exception ex)
            {
                set = Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Tickets_Con_Integridad()", "Error consultando tickets: " + ex.Message);
            }
            return set;
        }
        //DPA--

        //DPA++
        public DataSet Sincronizar_Tickets_Sin_Integridad(string _codTienda, DataSet TicketsDia)
        {
            DataSet set = new DataSet();
            NavisionDBAdapter adapterSet = new NavisionDBAdapter();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_Tickets_Sin_Integridad()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                this.DeleteOldOustandingTickets(_codTienda);
                if ((TicketsDia.Tables.Count > 0) && (TicketsDia.Tables[0].Rows.Count > 0))
                {
                    for (int num3 = 0; num3 <= (TicketsDia.Tables[0].Rows.Count - 1); num3++)
                    {
                        string tipoDocumento = TicketsDia.Tables[0].Rows[num3][0].ToString();
                        string documentNo = TicketsDia.Tables[0].Rows[num3][1].ToString();
                        string codTienda = TicketsDia.Tables[0].Rows[num3][2].ToString();
                        string codTPV = TicketsDia.Tables[0].Rows[num3][3].ToString();
                        string fechaDocumento = TicketsDia.Tables[0].Rows[num3][4].ToString();
                        string errorEnvio = TicketsDia.Tables[0].Rows[num3][5].ToString();
                        switch (tipoDocumento)
                        {
                            case "Factura":
                                if (!this.IsInvoiceInCentral(documentNo))
                                {
                                    if (errorEnvio == "No")
                                    {
                                        this.InsertTicketPendiente(tipoDocumento, documentNo, codTienda, codTPV, fechaDocumento, 1);
                                    }
                                }
                                else if (errorEnvio == "Sí")
                                {
                                    this.InsertTicketPendiente(tipoDocumento, documentNo, codTienda, codTPV, fechaDocumento, 0);
                                }
                                break;

                            case "Devolución":
                                if (!this.IsCrMemoInCentral(documentNo))
                                {
                                    if (errorEnvio == "No")
                                    {
                                        this.InsertTicketPendiente(tipoDocumento, documentNo, codTienda, codTPV, fechaDocumento, 1);
                                    }
                                }
                                else if (errorEnvio == "Sí")
                                {
                                    this.InsertTicketPendiente(tipoDocumento, documentNo, codTienda, codTPV, fechaDocumento, 0);
                                }
                                break;
                        }
                    }
                    set = this.GetTicketsPendientes(_codTienda);
                }
                else
                {
                    set = Utilidades.GenerarResultado("Nada que hacer.");
                }
            }
            catch (Exception ex)
            {
                set = Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Tickets_Sin_Integridad()", "Error consultando tickets: " + ex.Message);
            }
            return set;
        }
        //DPA--
        //DPA++
        public DataSet Incidencias_Crear(string NoIncidencia, string Origen, string Destino, string FechaRegistro, string HoraRegistro, string CodTienda, string CodTPV, string TipoIncidencia, string CodCliente, string NombreCliente, string ApellidosCliente, string CodVendedorRegistro, string NPedidoTransf, string NoTicket, string TPVSettle, DataSet DatosLineas)
        {
            System.Messaging.MessageQueueException exception;
            int num = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int num2 = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);
            string msqServerName = ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string msqsqltonavision = ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string msqsqlfromnavision = ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];
            DataSet set = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Incidencias_Crear()", "ERROR: No se ha validado, debe abrir login");
            }
            string numero = "";
            numero = NoIncidencia;
            try
            {
                NavisionDBNas nas = new NavisionDBNas();
                nas.NasInitializeChannels(msqServerName, msqsqlfromnavision, msqsqltonavision);
                string[] strArray = new string[] { numero, Origen, Destino, FechaRegistro, HoraRegistro, CodTienda, CodTPV, TipoIncidencia, CodCliente, NombreCliente, ApellidosCliente, CodVendedorRegistro, NPedidoTransf, NoTicket, TPVSettle };
                nas.ExecuteFunction = "PSTDGrabarCabeceraIncidencia";
                nas.Parameters = strArray;
                nas.SendParamsAsync(nas, "", false);
            }
            catch (System.Messaging.MessageQueueException exception1)
            {
                exception = exception1;
                //this.BorrarCabeceraINC(numero);
                return Utilidades.GenerarError(this.DBUser.UserCode, "Incidencias_Crear()", "Error al insertar la cabecera incidencia " + numero + ": " + exception.Message);
            }
            try
            {
                for (int num3 = 0; num3 <= (DatosLineas.Tables[0].Rows.Count - 1); num3++)
                {
                    NavisionDBNas nas3 = new NavisionDBNas();
                    nas3.NasInitializeChannels(msqServerName, msqsqlfromnavision, msqsqltonavision);
                    nas3.ExecuteFunction = "PSTDGrabarLineaIncidencia";
                    nas3.Parameters = new string[] { numero, DatosLineas.Tables[0].Rows[num3]["LineNo"].ToString(), DatosLineas.Tables[0].Rows[num3]["No."].ToString(), DatosLineas.Tables[0].Rows[num3]["CodVariante"].ToString(), DatosLineas.Tables[0].Rows[num3]["Cantidad"].ToString(), DatosLineas.Tables[0].Rows[num3]["MotivoDevolucion"].ToString(), DatosLineas.Tables[0].Rows[num3]["Comentario"].ToString(), DatosLineas.Tables[0].Rows[num3]["CodCliente"].ToString(), DatosLineas.Tables[0].Rows[num3]["Origen"].ToString(), DatosLineas.Tables[0].Rows[num3]["Destino"].ToString(), DatosLineas.Tables[0].Rows[num3]["CodVendedorRegistro"].ToString(), DatosLineas.Tables[0].Rows[num3]["NPedidoTransf"].ToString(), DatosLineas.Tables[0].Rows[num3]["TPVSettle"].ToString() };
                    nas3.SendParamsAsync(nas3, "", false);
                }
            }
            catch (System.Messaging.MessageQueueException exception3)
            {
                exception = exception3;
                set = Utilidades.GenerarError(this.DBUser.UserCode, "Incidencias_Crear()", "Error al insertar la incidencia " + numero + ": " + exception.Message);
                //this.BorrarLineasINC(numero);
                //this.BorrarCabeceraINC(numero);
                return set;
            }
            set = Utilidades.GenerarResultado("Correcto");
            set.Tables[0].Columns.Add("NoIncidencia", Type.GetType("System.String"));
            set.Tables[0].Rows[0]["NoIncidencia"] = numero;
            return set;
        }
        //DPA--

        public DataSet LinIncidencia_Update(string NoIncidencia, string LineNo, string ShopCode, string ShippingDate, string ShippingTime, string ShopComment, string Status)
        {
            System.Messaging.MessageQueueException exception;
            int num = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int num2 = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);
            string msqServerName = ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string msqsqltonavision = ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string msqsqlfromnavision = ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];
            DataSet set = new DataSet();

            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "LinIncidencia_Update()", "ERROR: No se ha validado, debe abrir login");
            }

            try
            {
                NavisionDBNas nas = new NavisionDBNas();
                nas.NasInitializeChannels(msqServerName, msqsqlfromnavision, msqsqltonavision);
                string[] strArray = new string[] { NoIncidencia, LineNo, ShopCode, ShippingDate, ShippingTime, ShopComment, Status };
                nas.ExecuteFunction = "PSTDActualizarLineaIncidencia";
                nas.Parameters = strArray;
                nas.SendParamsAsync(nas, "", false);
            }
            catch (System.Messaging.MessageQueueException exception1)
            {
                exception = exception1;
                //this.BorrarCabeceraINC(numero);
                return Utilidades.GenerarError(this.DBUser.UserCode, "LinIncidencia_Update()", "Error al actualizar la línea incidencia " + NoIncidencia + "-" + LineNo + ": " + exception.Message);
            }

            set = Utilidades.GenerarResultado("Correcto");
            set.Tables[0].Columns.Add("NoIncidencia", Type.GetType("System.String"));
            set.Tables[0].Rows[0]["NoIncidencia"] = NoIncidencia;
            return set;
        }

        //DPA++
        private DataSet GetTicketsPendientes(string codTienda)
        {
            DataSet set = new DataSet();
            NavisionDBTable ticketPendienteTable = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter ticketPendienteAdapter = new NavisionDBAdapter();
            ticketPendienteTable.TableNo = 50074; //Ticket pendiente
            ticketPendienteTable.AddColumn(10);
            ticketPendienteTable.AddColumn(20);
            ticketPendienteTable.AddColumn(30);
            ticketPendienteTable.AddColumn(40);
            ticketPendienteTable.AddColumn(50);
            ticketPendienteTable.AddColumn(60);
            ticketPendienteTable.AddFilter("Cód. tienda", codTienda);
            ticketPendienteAdapter.AddTable(ticketPendienteTable);
            ticketPendienteAdapter.Fill(ref set, false);
            Utilidades.CompletarDataSet(ref set, false, false);
            return set;
        }

        private bool IsTransferInCentral(string documentNo)
        {
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            table.TableNo = 5744; //Cab. transferencia envío
            table.AddColumn(1);
            table.AddFilter("Transfer Order No.", documentNo);
            adapter.AddTable(table);
            adapter.Fill(ref ds, false);
            Utilidades.CompletarDataSet(ref ds, false, false);
            return (ds.Tables[0].Rows.Count != 0);
        }


        public bool BorrarCabeceraINC(string Numero)
        {
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 50052; //Hist. Cab. Incidencia calidad
                table.AddFilter("No. Incidencia", Numero);
                adapter.AddTable(table);
                adapter.Fill(ref ds, true);
                Utilidades.CompletarDataSet(ref ds, false, false);
                adapter.DeleteItem = ds;
                try
                {
                    this.Connection.BWT();
                    adapter.Update();
                    this.Connection.EWT();
                }
                catch (Exception exception)
                {
                    this.Connection.AWT();
                    throw new Exception("BorrarCabeceraINC(2): " + exception.Message);
                }
            }
            catch (Exception exception2)
            {
                throw new Exception("BorrarCabeceraINC: " + exception2.Message);
            }
            return true;
        }
        public bool BorrarLineasINC(string Numero)
        {
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 50053; //Hist. Lin. Incidencia calidad
                table.AddFilter("No. Incidencia", Numero);
                adapter.AddTable(table);
                adapter.Fill(ref ds, true);
                Utilidades.CompletarDataSet(ref ds, false, false);
                adapter.DeleteItem = ds;
                try
                {
                    this.Connection.BWT();
                    adapter.Update();
                    this.Connection.EWT();
                }
                catch (Exception exception)
                {
                    this.Connection.AWT();
                    throw new Exception("BorrarLineasINC(2): " + exception.Message);
                }
            }
            catch (Exception exception2)
            {
                throw new Exception("BorrarLineasINC: " + exception2.Message);
            }
            return true;
        }

        private void InsertTicketPendiente(string tipoDocumento, string documentNo, string codTienda, string codTPV, string fechaDocumento, int errorEnvio)
        {
            NavisionDBTable errorTable = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter errorAdapter = new NavisionDBAdapter();
            DataSet TicketPendiente = new DataSet();
            errorTable.TableNo = 50074; //Ticket Pendiente
            errorAdapter.AddTable(errorTable);
            TicketPendiente = errorTable.GenerateStructure();
            DataRow TicketPendienteRow = TicketPendiente.Tables[0].NewRow();
            TicketPendienteRow["Tipo documento"] = tipoDocumento;
            TicketPendienteRow["Nº documento"] = documentNo;
            TicketPendienteRow["Cód. tienda"] = codTienda;
            TicketPendienteRow["Cód. TPV"] = codTPV;
            TicketPendienteRow["Fecha documento"] = fechaDocumento;
            TicketPendienteRow["Error envío"] = errorEnvio;
            TicketPendiente.Tables[0].Rows.Add(TicketPendienteRow);
            errorAdapter.InsertItem = TicketPendiente;
            try
            {
                errorTable.ConnectionDB.BWT();
                errorAdapter.Update();
                errorTable.ConnectionDB.EWT();
            }
            catch (Exception)
            {
                errorTable.ConnectionDB.AWT();
            }
        }

        private void DeleteOldOustandingTickets(string codTienda)
        {
            DataSet set = new DataSet();
            NavisionDBTable ticketPendienteTable = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter ticketPendienteAdapter = new NavisionDBAdapter();
            ticketPendienteTable.TableNo = 50074; //Ticket Pendiente
            ticketPendienteTable.AddFilter("Cód. tienda", codTienda);
            ticketPendienteAdapter.AddTable(ticketPendienteTable);
            ticketPendienteAdapter.Fill(ref set, true);
            ticketPendienteAdapter.DeleteItem = set;
            try
            {
                ticketPendienteTable.ConnectionDB.BWT();
                ticketPendienteAdapter.Update();
                ticketPendienteTable.ConnectionDB.EWT();
            }
            catch (Exception)
            {
                ticketPendienteTable.ConnectionDB.AWT();
            }
        }

        private bool IsInvoiceInCentralOK(string documentNo, int numLineas)
        {
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            table.TableNo = 112; //Histórico cab. factura venta
            table.AddColumn(3);
            table.AddFilter("No.", documentNo);
            adapter.AddTable(table);
            adapter.Fill(ref ds, false);
            Utilidades.CompletarDataSet(ref ds, false, false);
            if (ds.Tables[0].Rows.Count == 0)
            {
                table = new NavisionDBTable(this.Connection, this.DBUser);
                adapter = new NavisionDBAdapter();
                ds = new DataSet();
                table.TableNo = 50001; //Mensajes de TPV
                table.AddColumn(5);
                table.AddColumn(8);
                table.AddColumn(9);
                table.AddFilter("Nº Documento", documentNo);
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                if (ds.Tables[0].Rows.Count == 0)
                {
                    return false;
                }
                bool msgPSTDGrabarCabeceraFacturaOk = false;
                //bool msgPSTDInformacionEnvioOk = false;
                bool msgPSTDGrabarLineaFacturaOK = false;
                bool msgPSTDMultiFormaOK = false;
                //bool msgPSTDMultiForma2OK = false;
                //bool msgPSTDMultiForma3OK = false;
                bool msgActualizaPreciosVentaOK = false;
                bool msgPSTDFacturarOK = false;
                bool msgPSTDInfoTicketFacturaOK = false;
                int linesCount = 0;
                foreach (DataRow currRow in ds.Tables[0].Rows)
                {
                    bool errorEnvio;
                    if (currRow[0].ToString().StartsWith("PSTDGrabarCabeceraFactura"))
                    {
                        errorEnvio = false;
                        if (bool.TryParse(currRow[1].ToString(), out errorEnvio) && !errorEnvio)
                        {
                            table = new NavisionDBTable(this.Connection, this.DBUser);
                            adapter = new NavisionDBAdapter();
                            ds = new DataSet();
                            table.TableNo = 36; //Cab. venta
                            table.AddColumn(3);
                            table.AddFilter("No.", documentNo);
                            adapter.AddTable(table);
                            adapter.Fill(ref ds, false);
                            Utilidades.CompletarDataSet(ref ds, false, false);
                            if (ds.Tables[0].Rows.Count > 0)
                            {
                                msgPSTDGrabarCabeceraFacturaOk = true;
                            }
                        }
                        else
                        {
                            msgPSTDGrabarCabeceraFacturaOk = true;
                        }
                    }
                    //else if (currRow[0].ToString().StartsWith("PSTDInformacionEnvio"))
                    //{
                    //    msgPSTDInformacionEnvioOk = true;
                    //}
                    else if (currRow[0].ToString().StartsWith("PSTDGrabarLineaFactura"))
                    {
                        linesCount++;
                        errorEnvio = false;
                        if (bool.TryParse(currRow[1].ToString(), out errorEnvio) && !errorEnvio)
                        {
                            table = new NavisionDBTable(this.Connection, this.DBUser);
                            adapter = new NavisionDBAdapter();
                            ds = new DataSet();
                            table.TableNo = 37; //Lín. venta
                            table.AddColumn(3);
                            table.AddFilter("Document No.", documentNo);
                            adapter.AddTable(table);
                            adapter.Fill(ref ds, false);
                            Utilidades.CompletarDataSet(ref ds, false, false);
                            if (ds.Tables[0].Rows.Count > 0)
                            {
                                msgPSTDGrabarLineaFacturaOK = true;
                            }
                        }
                        else
                        {
                            msgPSTDGrabarLineaFacturaOK = true;
                        }
                    }
                    else if (currRow[0].ToString().StartsWith("PSTDMultiForma("))
                    {
                        errorEnvio = false;
                        if (bool.TryParse(currRow[1].ToString(), out errorEnvio) && !errorEnvio)
                        {
                            table = new NavisionDBTable(this.Connection, this.DBUser);
                            adapter = new NavisionDBAdapter();
                            ds = new DataSet();
                            table.TableNo = 50003; //Multiforma de Pago
                            table.AddColumn(1);
                            table.AddFilter("Nº Documento", documentNo);
                            adapter.AddTable(table);
                            adapter.Fill(ref ds, false);
                            Utilidades.CompletarDataSet(ref ds, false, false);
                            if (ds.Tables[0].Rows.Count > 0)
                            {
                                msgPSTDMultiFormaOK = true;
                            }
                        }
                        else
                        {
                            msgPSTDMultiFormaOK = true;
                        }
                    }
                    //else if (currRow[0].ToString().StartsWith("PSTDMultiForma2("))
                    //{
                    //    msgPSTDMultiForma2OK = true;
                    //}
                    //else if (currRow[0].ToString().StartsWith("PSTDMultiForma3("))
                    //{
                    //    msgPSTDMultiForma3OK = true;
                    //}
                    else if (currRow[0].ToString().StartsWith("ActualizaPreciosVenta"))
                    {
                        msgActualizaPreciosVentaOK = true;
                    }
                    else if (currRow[0].ToString().StartsWith("PSTDFacturar"))
                    {
                        bool tempBool = false;
                        if (bool.TryParse(currRow[1].ToString(), out tempBool))
                        {
                            msgPSTDFacturarOK = tempBool;
                        }
                    }
                    else if (currRow[0].ToString().StartsWith("PSTDInfoTicketFactura"))
                    {
                        msgPSTDInfoTicketFacturaOK = true;
                    }
                }
                if (linesCount < numLineas)
                {
                    msgPSTDGrabarLineaFacturaOK = false;
                }
                return (msgPSTDGrabarCabeceraFacturaOk && msgPSTDGrabarLineaFacturaOK && msgPSTDMultiFormaOK && msgActualizaPreciosVentaOK && msgPSTDFacturarOK && msgPSTDInfoTicketFacturaOK);
            }
            return true;
        }

        private bool IsCrMemoInCentralOK(string documentNo, int numLineas)
        {
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            table.TableNo = 114; //Histórico cab. abono venta
            table.AddColumn(3);
            table.AddFilter("No.", documentNo);
            adapter.AddTable(table);
            adapter.Fill(ref ds, false);
            Utilidades.CompletarDataSet(ref ds, false, false);
            if (ds.Tables[0].Rows.Count == 0)
            {
                table = new NavisionDBTable(this.Connection, this.DBUser);
                adapter = new NavisionDBAdapter();
                ds = new DataSet();
                table.TableNo = 50001; //Mensajes de TPV
                table.AddColumn(5);
                table.AddColumn(8);
                table.AddColumn(9);
                table.AddFilter("Nº Documento", documentNo);
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                if (ds.Tables[0].Rows.Count == 0)
                {
                    return false;
                }
                bool msgPSTDGrabarCabeceraAbonoOk = false;
                //bool msgPSTDInformacionEnvioOk = false;
                bool msgPSTDGrabarLineaAbonoOK = false;
                bool msgPSTDMultiFormaOK = false;
                bool msgPSTDMultiForma2OK = false;
                //bool msgActualizaPreciosVentaOK = false;
                bool msgPSTDAbonarOK = false;
                bool msgPSTDInfoTicketAbonoOK = false;
                int linesCount = 0;
                foreach (DataRow currRow in ds.Tables[0].Rows)
                {
                    bool errorEnvio;
                    if (currRow[0].ToString().StartsWith("PSTDAbonoGenerar2"))
                    {
                        errorEnvio = false;
                        if (bool.TryParse(currRow[1].ToString(), out errorEnvio) && !errorEnvio)
                        {
                            table = new NavisionDBTable(this.Connection, this.DBUser);
                            adapter = new NavisionDBAdapter();
                            ds = new DataSet();
                            table.TableNo = 36; //Cab. venta
                            table.AddColumn(3);
                            table.AddFilter("No.", documentNo);
                            adapter.AddTable(table);
                            adapter.Fill(ref ds, false);
                            Utilidades.CompletarDataSet(ref ds, false, false);
                            if (ds.Tables[0].Rows.Count > 0)
                            {
                                msgPSTDGrabarCabeceraAbonoOk = true;
                            }
                        }
                        else
                        {
                            msgPSTDGrabarCabeceraAbonoOk = true;
                        }
                    }
                    //else if (currRow[0].ToString().StartsWith("PSTDInformacionEnvio"))
                    //{
                    //    msgPSTDInformacionEnvioOk = true;
                    //}
                    else if (currRow[0].ToString().StartsWith("PSTDGrabarLineaAbono2"))
                    {
                        linesCount++;
                        errorEnvio = false;
                        if (bool.TryParse(currRow[1].ToString(), out errorEnvio) && !errorEnvio)
                        {
                            table = new NavisionDBTable(this.Connection, this.DBUser);
                            adapter = new NavisionDBAdapter();
                            ds = new DataSet();
                            table.TableNo = 37; //Lín. venta
                            table.AddColumn(3);
                            table.AddFilter("Document No.", documentNo);
                            adapter.AddTable(table);
                            adapter.Fill(ref ds, false);
                            Utilidades.CompletarDataSet(ref ds, false, false);
                            if (ds.Tables[0].Rows.Count > 0)
                            {
                                msgPSTDGrabarLineaAbonoOK = true;
                            }
                        }
                        else
                        {
                            msgPSTDGrabarLineaAbonoOK = true;
                        }
                    }
                    else if (currRow[0].ToString().StartsWith("PSTDMultiForma("))
                    {
                        errorEnvio = false;
                        if (bool.TryParse(currRow[1].ToString(), out errorEnvio) && !errorEnvio)
                        {
                            table = new NavisionDBTable(this.Connection, this.DBUser);
                            adapter = new NavisionDBAdapter();
                            ds = new DataSet();
                            table.TableNo = 50003; //Multiforma de Pago
                            table.AddColumn(1);
                            table.AddFilter("Nº Documento", documentNo);
                            adapter.AddTable(table);
                            adapter.Fill(ref ds, false);
                            Utilidades.CompletarDataSet(ref ds, false, false);
                            if (ds.Tables[0].Rows.Count > 0)
                            {
                                msgPSTDMultiFormaOK = true;
                            }
                        }
                        else
                        {
                            msgPSTDMultiFormaOK = true;
                        }
                    }
                    else if (currRow[0].ToString().StartsWith("PSTDMultiForma2("))
                    {
                        msgPSTDMultiForma2OK = true;
                    }
                    //else if (currRow[0].ToString().StartsWith("ActualizaPreciosVenta"))
                    //{
                    //    msgActualizaPreciosVentaOK = true;
                    //}
                    else if (currRow[0].ToString().StartsWith("PSTDAbonar2"))
                    {
                        errorEnvio = false;
                        if (bool.TryParse(currRow[1].ToString(), out errorEnvio))
                        {
                            msgPSTDAbonarOK = errorEnvio;
                        }
                    }
                    else if (currRow[0].ToString().StartsWith("PSTDInfoTicketAbono"))
                    {
                        msgPSTDInfoTicketAbonoOK = true;
                    }
                }
                if (linesCount < numLineas)
                {
                    msgPSTDGrabarLineaAbonoOK = false;
                }
                return (msgPSTDGrabarCabeceraAbonoOk && msgPSTDGrabarLineaAbonoOK && msgPSTDMultiFormaOK && msgPSTDMultiForma2OK && msgPSTDAbonarOK && msgPSTDInfoTicketAbonoOK);
            }
            return true;
        }

        private bool IsInvoiceInCentral(string documentNo)
        {
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBTable table2 = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            table.TableNo = 112; //Histórico cab. factura venta
            table.AddColumn(3);
            table.AddFilter("No.", documentNo);
            adapter.AddTable(table);
            table2.TableNo = 50001; //Mensajes de TPV
            table2.AddColumn(9);
            table2.AddFilter("Nº Documento", documentNo);
            table2.AddFilter("Fallo", "true");
            adapter.AddTable(table2);
            adapter.Fill(ref ds, false);
            Utilidades.CompletarDataSet(ref ds, false, false);
            return ((ds.Tables[0].Rows.Count != 0) || (ds.Tables[1].Rows.Count != 0));
        }

        private bool IsCrMemoInCentral(string documentNo)
        {
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBTable table2 = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            table.TableNo = 114; //Histórico cab. abono venta
            table.AddColumn(3);
            table.AddFilter("No.", documentNo);
            adapter.AddTable(table);
            table2.TableNo = 50001; //Mensajes de TPV
            table2.AddColumn(9);
            table2.AddFilter("Nº Documento", documentNo);
            table2.AddFilter("Fallo", "true");
            adapter.AddTable(table2);
            adapter.Fill(ref ds, false);
            Utilidades.CompletarDataSet(ref ds, false, false);
            return ((ds.Tables[0].Rows.Count != 0) || (ds.Tables[1].Rows.Count != 0));
        }

        public DataSet Get_Reaprovision_Tienda_Pendiente_Ind(string Tienda)
        {
            return this.Obtener_Reaprovision_Tienda_Independiente(Tienda, "", false);
        }

        public DataSet Get_Reaprovision_Tienda_Pendiente_DiaProd(string Tienda)
        {
            return this.Obtener_Reaprovision_Tienda(Tienda, "", false);
        }

        public DataSet Sincronizar_Transferencias(string _codTienda, DataSet Transferencias)
        {
            return this.Sincronizar_Transferencias_Entre_Fechas(_codTienda, "", "", Transferencias);
        }


        public DataSet Sincronizar_Transferencias_Entre_Fechas(string _codTienda, string fechaInicio, string fechaFin, DataSet Transferencias)
        {
            DataSet set = new DataSet();
            NavisionDBAdapter adapterSet = new NavisionDBAdapter();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_Transferencias()", "ERROR: No se ha validado, debe abrir login");
            }
            if (this.TransfersNotDownloadedInLocation(_codTienda, fechaInicio, fechaFin))
            {
                return Utilidades.GenerarError("", "Sincronizar_Transferencias()", "Error: Existen transferencias pendientes. Es necesario que obtenga y registre todas las transferencias pendientes.");
            }
            try
            {
                if ((Transferencias.Tables.Count > 0) && (Transferencias.Tables[0].Rows.Count > 0))
                {
                    for (int num3 = 0; num3 <= (Transferencias.Tables[0].Rows.Count - 1); num3++)
                    {
                        string documentNo = Transferencias.Tables[0].Rows[num3][0].ToString();
                        string codTienda = Transferencias.Tables[0].Rows[num3][1].ToString();
                        string codTPV = Transferencias.Tables[0].Rows[num3][2].ToString();
                        if (this.IsTransferInCentral(documentNo))
                        {
                            this.MarkShipmentReceived(documentNo);
                        }
                    }
                    set = Utilidades.GenerarResultado("Se han marcado los envios ya recibidos correctamente.");
                }
                else
                {
                    set = Utilidades.GenerarResultado("Nada que hacer.");
                }
            }
            catch (Exception ex)
            {
                set = Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Transferencias()", "Error consultando tickets: " + ex.Message);
            }
            return set;
        }

        private void MarkShipmentReceived(string documentNo)
        {
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            table.TableNo = 5744; //Cab. transferencia envío
            table.AddFilter("Transfer Order No.", documentNo);
            table.Modify(50070, true);
            table.Modify(50071, false);
            adapter.AddTable(table);
            try
            {
                table.ConnectionDB.BWT();
                adapter.Update();
                table.ConnectionDB.EWT();
            }
            catch (Exception)
            {
                table.ConnectionDB.AWT();
            }
        }

        public DataSet TraerLineasIncidencia(string codTienda, string fechaInicio, string fechaFin)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "TraerLineasIncidencia()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 50053; //Hist. Lin. Incidencia calidad
                table.AddColumn("No. Incidencia");
                table.AddColumn("Line No.");                                
                table.AddColumn("Last Update Date");
                table.AddColumn("Receipt Date");
                table.AddColumn("Receipt Time");                                
                table.AddColumn("Status");
                table.AddColumn("Solution");
                table.AddColumn("BYL Comments");
                table.AddColumn("Promised Delivery Date");
                table.AddColumn("Solution Date");
                table.AddColumn("Solution Time");
                table.AddColumn("BYL Shipping Date");
                table.AddColumn("BYL Shipping Time");

                table.AddFilter("Source Location", codTienda);
                table.AddFilter("Last Update Date", Utilidades.FechaDesde(fechaInicio) + ".." + Utilidades.FechaHasta(fechaFin));

                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "TraerLineasIncidenciaCliente(): ", exception.Message);
            }
        }

        public DataSet Sincronizar_Motivo_Devolucion(string CodTienda)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Sincronizar_Motivo_Devolucion()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 6635; //Causa devolución
                table.AddColumn(1);
                table.AddColumn(2);
                table.AddColumn(3);
                table.AddColumn(4);
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_Motivo_Devolucion(): ", exception.Message);
            }
        }

        public DataSet Buscar_Clientes_Contador(string Nombre, string Apellidos, string NIF, string Telefono)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Buscar_Clientes_Contador()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand command = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader reader = new NavisionDBDataReader();
                DataSet set = new DataSet();
                table.TableNo = 18; //Cliente
                table.AddColumn(86);
                if (Nombre != "")
                {
                    table.AddFilter(4, "*" + Nombre + "*");
                }
                if (Apellidos != "")
                {
                    table.AddFilter(2, "*" + Apellidos + "*");
                }
                if (NIF != "")
                {
                    table.AddFilter(86, "*" + NIF + "*");
                }
                if (Telefono != "")
                {
                    table.AddFilter(9, "*" + Telefono + "*");
                }
                table.KeyInNavisionFormat = "VAT Registration No.";
                command.Table = table;

                return Utilidades.GenerarResultado(Convert.ToString(command.ExecuteReader(false).RecordsAffected));
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Buscar_Clientes_Contador()", exception.Message);
            }
        }

        public DataSet Buscar_Clientes(string Nombre, string Apellidos, string NIF, string Telefono)
        {
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "Buscar_Clientes()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter adapter = new NavisionDBAdapter();
                DataSet ds = new DataSet();
                table.TableNo = 18; //Cliente
                table.AddColumn(1);
                table.AddColumn(2);
                table.AddColumn(4);
                table.AddColumn(5);
                table.AddColumn(91);
                table.AddColumn(7);
                table.AddColumn(92);
                table.AddColumn(9);
                table.AddColumn(102);
                table.AddColumn(86);
                table.AddColumn(47);
                table.AddColumn(27);
                table.AddColumn(34);
                if (Nombre != "")
                {
                    table.AddFilter(4, "*" + Nombre + "*");
                }
                if (Apellidos != "")
                {
                    table.AddFilter(2, "*" + Apellidos + "*");
                }
                if (NIF != "")
                {
                    table.AddFilter(86, "*" + NIF + "*");
                }
                if (Telefono != "")
                {
                    table.AddFilter(9, "*" + Telefono + "*");
                }
                table.KeyInNavisionFormat = "VAT Registration No.";
                adapter.AddTable(table);
                adapter.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                return ds;
            }
            catch (Exception exception)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Buscar_Clientes()", exception.Message);
            }
        }

        public DataSet MarkShipmentNotReceived(string _codTienda, string fechaInicio, string fechaFin)
        {
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            DataSet set = new DataSet();
            table.TableNo = 5744; //Cab. transferencia envío
            table.AddFilter("Transfer-to Code", _codTienda);
            table.AddFilter("Received In TPV", "false");
            if ((fechaInicio != "") || (fechaFin != ""))
            {
                table.AddFilter("Posting Date", fechaInicio + ".." + fechaFin);
            }
            table.Modify(50071, true);
            adapter.AddTable(table);
            try
            {
                table.ConnectionDB.BWT();
                adapter.Update();
                table.ConnectionDB.EWT();
                return Utilidades.GenerarResultado("Se han marcado los envios no recibidos.");
            }
            catch (Exception ex)
            {
                table.ConnectionDB.AWT();
                return Utilidades.GenerarError(_codTienda + "[" + fechaInicio + ".." + fechaFin + "]" ,"MarkShipmentNotReceived()", ex.Message);
            }
        }


        private bool TransfersNotDownloadedInLocation(string codTienda, string fechaInicio, string fechaFin)
        {
            NavisionDBTable table = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter adapter = new NavisionDBAdapter();
            DataSet ds = new DataSet();
            table.TableNo = 83; //Lín. diario producto
            table.KeyInNavisionFormat = "Almacen_Destino,Tipo_Linea_TPV";
            table.AddColumn(1);
            table.AddFilter("Almacen_Destino", codTienda);
            table.AddFilter("Tipo_Linea_TPV", "Transferencia");
            if ((fechaInicio != "") || (fechaFin != ""))
            {
                table.AddFilter("Posting Date", fechaInicio + ".." + fechaFin);
            }
            adapter.AddTable(table);
            adapter.Fill(ref ds, false);
            Utilidades.CompletarDataSet(ref ds, false, false);
            return (ds.Tables[0].Rows.Count != 0);
        }








        //DPA--
        public DataSet Facturas_Crear(string NumeroTicket,
                                        string Cliente,
                                        string CodTienda,
                                        string CodDirEnvio,
                                        DataSet DatosLineas,
                                        string EnvioNombre,
                                        string EnvioDireccion,
                                        string EnvioCP,
                                        string EnvioPoblacion,
                                        string EnvioProvincia,
                                        string EnvioAtencion,
                                        string CodFormaPago,
                                        string CodVendedor,
                                        string FechaTicket,
                                        string CodTPV,
                                        string HoraTicket,
                                        string TotalaPagar,
                                        string ImporteVenta,
                                        string ImporteEntregado,
                                        string NumeroReserva,
                                        string AplicarDto,
                                        string NumTarjetaDto,
                                        string Financiera,
                                        string GruDtoCol,
                                        string NumAutFinan,
                                        string ImpGtoFinan,
                                        string NumFidelizacion,
                                        string CrearEntrega,
                                        string CobroTrans,
                                        string FactAuto,
                                        string RepInmediata,
                                        string DesColectivo,
                                        string NumFacAbonar,
                                        string EnvioObserv,
                                        string ImpCobrarEntregas,
                                        string ImpLiqAnticipo,
                                        string FechaEnvio,
                                        DataSet MultiForma)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "VentasCrear()", "ERROR: No se ha validado, debe abrir login");

            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumeroTicket;

            //LLAMAMOS A LA FUNCION DE CREAR FACTURA
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[29];

                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = Cliente;
                MyArray[2] = CodTienda;
                MyArray[3] = CodFormaPago;
                MyArray[4] = CodVendedor;
                MyArray[5] = FechaTicket;
                MyArray[6] = AplicarDto;
                MyArray[7] = NumTarjetaDto;
                MyArray[8] = GruDtoCol;
                MyArray[9] = NumFidelizacion;
                MyArray[10] = ImporteEntregado;
                MyArray[11] = NumeroReserva;
                MyArray[12] = CrearEntrega;
                MyArray[13] = CobroTrans;
                MyArray[14] = FactAuto;
                MyArray[15] = RepInmediata;
                MyArray[16] = DesColectivo;
                MyArray[17] = NumFacAbonar;
                MyArray[18] = ImpCobrarEntregas;
                MyArray[19] = ImpLiqAnticipo;                
                MyArray[20] = DatosLineas.Tables[0].Rows.Count.ToString();

                //BYL: Se elimina PSTDInformacionEnvio
                MyArray[21] = EnvioNombre;
                MyArray[22] = EnvioDireccion;
                MyArray[23] = EnvioCP;
                MyArray[24] = EnvioPoblacion;
                MyArray[25] = EnvioProvincia;
                MyArray[26] = EnvioAtencion;
                MyArray[27] = EnvioObserv;
                MyArray[28] = FechaEnvio;
                //BYL: Se elimina PSTDInformacionEnvio

                nas.ExecuteFunction = "PSTDGrabarCabeceraFactura";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);


            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LOS DATOS DE ENVIO O RECOGIDA EN CASO DE ABONOS
            /* BYL: Se elimina PSTDInformacionEnvio
            try
            {
                NavisionDB.NavisionDBNas NasDatosEnvio = new NavisionDB.NavisionDBNas();
                NasDatosEnvio.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasDatosEnvio.ExecuteFunction = "PSTDInformacionEnvio";
                string[] MyArray1 = new string[11];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = Cliente;
                MyArray1[2] = "Factura";
                MyArray1[3] = EnvioNombre;
                MyArray1[4] = EnvioDireccion;
                MyArray1[5] = EnvioCP;
                MyArray1[6] = EnvioPoblacion;
                MyArray1[7] = EnvioProvincia;
                MyArray1[8] = EnvioAtencion;
                MyArray1[9] = EnvioObserv;
                MyArray1[10] = FechaEnvio;
                NasDatosEnvio.Parameters = MyArray1;
                NasDatosEnvio.SendParamsAsync(NasDatosEnvio, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al introducir datos envio: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }
            */

            //LLAMAMOS A LA FUNCION DE CREAR LAS LINEAS DE FACTURA
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTDGrabarLineaFactura";
                    string[] MyArray1 = new string[19];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Quantity"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["UnitPrice"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["LineDiscount"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Description"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["Type"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["NumCheque"].ToString();
                    MyArray1[8] = DatosLineas.Tables[0].Rows[i]["NumChequeLiq"].ToString();
                    MyArray1[9] = DatosLineas.Tables[0].Rows[i]["CrearEntrega"].ToString();
                    MyArray1[10] = DatosLineas.Tables[0].Rows[i]["CdadEntregar"].ToString();
                    MyArray1[11] = DatosLineas.Tables[0].Rows[i]["NumLinAbonar"].ToString();
                    MyArray1[12] = DatosLineas.Tables[0].Rows[i]["NumPack"].ToString();
                    MyArray1[13] = DatosLineas.Tables[0].Rows[i]["SecPack"].ToString();
                    MyArray1[14] = DatosLineas.Tables[0].Rows[i]["DtoCol"].ToString();
                    MyArray1[15] = DatosLineas.Tables[0].Rows[i]["ImpDtoCol"].ToString();
                    MyArray1[16] = DatosLineas.Tables[0].Rows[i]["NumLinea"].ToString();
                    MyArray1[17] = DatosLineas.Tables[0].Rows[i]["Variante"].ToString();
                    //>>EB.ICP.20100601
                    MyArray1[18] = DatosLineas.Tables[0].Rows[i]["CodVend"].ToString();
                    //<<EB.ICP.20100601
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DE LA FACTURA
            try
            {

                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma = new NavisionDB.NavisionDBNas();
                        NasMultiForma.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma.ExecuteFunction = "PSTDMultiForma";
                        string[] MyArray1 = new string[25];
                        MyArray1[0] = "Factura";
                        MyArray1[1] = NumeroSerieNuevo;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][1].ToString(); // Cod.Forma Pago
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][2].ToString(); // Importe
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][3].ToString(); // Cambio
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][4].ToString(); // Nº Vale
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][5].ToString(); // Tipo de Pago
                        MyArray1[8] = CodTPV;
                        MyArray1[9] = CodTienda;
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][6].ToString(); // Cobrado
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][7].ToString(); // Nº Vale Reserva
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][8].ToString(); // Nº cheque regalo
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][9].ToString(); // Tipo de vale

                        //BYL: Se elimina PSTDMultiForma2 
                        MyArray1[14] = MultiForma.Tables[0].Rows[i][10].ToString();// Nº tarjeta dto.
                        MyArray1[15] = MultiForma.Tables[0].Rows[i][11].ToString();// Financiera
                        MyArray1[16] = MultiForma.Tables[0].Rows[i][12].ToString();// Nº autorización financiera
                        MyArray1[17] = MultiForma.Tables[0].Rows[i][13].ToString();// "Gastos financiación"
                        MyArray1[18] = MultiForma.Tables[0].Rows[i][14].ToString();// % Gastos financiación
                        MyArray1[19] = MultiForma.Tables[0].Rows[i][15].ToString();// Cliente que financia
                        MyArray1[20] = MultiForma.Tables[0].Rows[i][16].ToString();// Forma pago gastos financieros
                        MyArray1[21] = MultiForma.Tables[0].Rows[i][17].ToString();// Cta. contrapartida gts. finan.
                        MyArray1[22] = MultiForma.Tables[0].Rows[i][18].ToString();// Nº tarjeta de cobro
                        MyArray1[23] = MultiForma.Tables[0].Rows[i][19].ToString();// Fecha registro
                        MyArray1[24] = MultiForma.Tables[0].Rows[i][20].ToString();// Nº tarjeta cobro gastos financieros
                        //BYL: Se elimina PSTDMultiForma2 

                        NasMultiForma.Parameters = MyArray1;
                        NasMultiForma.SendParamsAsync(NasMultiForma, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DEL DOCUMENTO
            //CON EL RESTO DE INFORMACION QUE FALTA
            
            /* ### BYL: Se elimina PSTDMultiForma2  ###
            try
            {

                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma2 = new NavisionDB.NavisionDBNas();
                        NasMultiForma2.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma2.ExecuteFunction = "PSTDMultiForma2";
                        string[] MyArray1 = new string[14];
                        MyArray1[0] = "Factura";
                        MyArray1[1] = NumeroSerieNuevo;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][10].ToString();// Nº tarjeta dto.
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][11].ToString();// Financiera
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][12].ToString();// Nº autorización financiera
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][13].ToString();// "Gastos financiación"
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][14].ToString();// % Gastos financiación
                        MyArray1[8] = MultiForma.Tables[0].Rows[i][15].ToString();// Cliente que financia
                        MyArray1[9] = MultiForma.Tables[0].Rows[i][16].ToString();// Forma pago gastos financieros
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][17].ToString();// Cta. contrapartida gts. finan.
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][18].ToString();// Nº tarjeta de cobro
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][19].ToString();// Fecha registro
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][20].ToString();// Nº tarjeta cobro gastos financieros
                        NasMultiForma2.Parameters = MyArray1;
                        NasMultiForma2.SendParamsAsync(NasMultiForma2, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al insertar multiformas2: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }
            */

            //LLAMAMOS A LA FUNCION DE RECALCULAR LINEAS PARA LAS LINEAS QUE CONTIENEN PACK
            /* ### BYL: Se elimina ActualizaPreciosVenta  ###
            try
            {
                NavisionDB.NavisionDBNas NasRecalcularLineas = new NavisionDB.NavisionDBNas();
                NasRecalcularLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasRecalcularLineas.ExecuteFunction = "ActualizaPreciosVenta";
                string[] MyArray1 = new string[2];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = "2";
                NasRecalcularLineas.Parameters = MyArray1;
                NasRecalcularLineas.SendParamsAsync(NasRecalcularLineas, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al recalcular línea factura: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }
            */

            //LLAMAMOS A LA FUNCION FACTURAR
            try
            {

                NavisionDB.NavisionDBNas NasFacturar = new NavisionDB.NavisionDBNas();
                NasFacturar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasFacturar.ExecuteFunction = "PSTDFacturar";
                string[] MyArray1 = new string[2];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = NumeroReserva;
                NasFacturar.Parameters = MyArray1;
                NasFacturar.SendParamsAsync(NasFacturar, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE CREAR LA INFORMACION DE LA TABLA TICKET
            try
            {

                NavisionDB.NavisionDBNas NasTicket = new NavisionDB.NavisionDBNas();
                NasTicket.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasTicket.ExecuteFunction = "PSTDInfoTicketFactura";
                string[] MyArray1 = new string[10];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = CodTienda;
                MyArray1[2] = CodTPV;
                MyArray1[3] = FechaTicket;
                MyArray1[4] = HoraTicket;
                MyArray1[5] = ImporteVenta;
                MyArray1[6] = TotalaPagar;
                MyArray1[7] = ImporteEntregado;
                MyArray1[8] = CodVendedor;
                MyArray1[9] = Cliente;

                NasTicket.Parameters = MyArray1;
                NasTicket.SendParamsAsync(NasTicket, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al insertar tabla ticket: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet Abono_Crear2(string NumeroTicket,
                                string Cliente,
                                string CodTienda,
                                DataSet DatosLineas,
                                string EnvioNombre,
                                string EnvioDireccion,
                                string EnvioCP,
                                string EnvioPoblacion,
                                string EnvioProvincia,
                                string CodFormaPago,
                                string CodVendedor,
                                string FechaTicket,
                                string CodTPV,
                                string HoraTicket,
                                string TotalaPagar,
                                string ImporteVenta,
                                string AplicarDto,
                                string NumTarjetaDto,
                                string GruDtoCol,
                                string NumFidelizacion,
                                string CrearEntrega,
                                string CobroTrans,
                                string FactAuto,
                                string RepInmediata,
                                string NumFacAbonar,
                                string EnvioObserv,
                                string DesColectivo,
                                string FechaEnvio,
                                DataSet MultiForma)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Abono_Crear2()", "ERROR: No se ha validado, debe abrir login");

            //NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            //NavisionDBAdapter Da = new NavisionDBAdapter();
            //NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            //NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            //NavisionDBDataReader Rd = new NavisionDBDataReader();

            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumeroTicket;

            //LLAMAMOS A LA FUNCION DE CREAR FACTURA
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[25];
                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = Cliente;
                MyArray[2] = CodTienda;
                MyArray[3] = CodFormaPago;
                MyArray[4] = CodVendedor;
                MyArray[5] = FechaTicket;
                MyArray[6] = NumFacAbonar;
                MyArray[7] = CrearEntrega;
                MyArray[8] = AplicarDto;
                MyArray[9] = NumTarjetaDto;
                MyArray[10] = GruDtoCol;
                MyArray[11] = NumFidelizacion;
                MyArray[12] = CobroTrans;
                MyArray[13] = FactAuto;
                MyArray[14] = RepInmediata;
                MyArray[15] = DesColectivo;
                MyArray[16] = DatosLineas.Tables[0].Rows.Count.ToString();

                //BYL: Se elimina PSTDInformacionEnvio
                MyArray[17] = EnvioNombre;
                MyArray[18] = EnvioDireccion;
                MyArray[19] = EnvioCP;
                MyArray[20] = EnvioPoblacion;
                MyArray[21] = EnvioProvincia;
                MyArray[22] = "";
                MyArray[23] = EnvioObserv;
                MyArray[24] = FechaEnvio;
                //BYL: Se elimina PSTDInformacionEnvio

                nas.ExecuteFunction = "PSTDAbonoGenerar2";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);


            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Crear2()", "Error al insertar la cabecera abono: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LOS DATOS DE ENVIO O RECOGIDA EN CASO DE ABONOS
            /* BYL: Se elimina PSTDInformacionEnvio
            try
            {
                NavisionDB.NavisionDBNas NasDatosEnvio = new NavisionDB.NavisionDBNas();
                NasDatosEnvio.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasDatosEnvio.ExecuteFunction = "PSTDInformacionEnvio";
                string[] MyArray1 = new string[11];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = Cliente;
                MyArray1[2] = "Abono";
                MyArray1[3] = EnvioNombre;
                MyArray1[4] = EnvioDireccion;
                MyArray1[5] = EnvioCP;
                MyArray1[6] = EnvioPoblacion;
                MyArray1[7] = EnvioProvincia;
                MyArray1[8] = "";
                MyArray1[9] = EnvioObserv;
                MyArray1[10] = FechaEnvio;

                NasDatosEnvio.Parameters = MyArray1;
                NasDatosEnvio.SendParamsAsync(NasDatosEnvio, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Crear2()", "Error al introducir datos envio: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }
            */

            //LLAMAMOS A LA FUNCION DE CREAR LAS LINEAS DE FACTURA
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTDGrabarLineaAbono2";
                    string[] MyArray1 = new string[17];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Quantity"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["UnitPrice"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["LineDiscount"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Description"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["Type"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["CrearEntrega"].ToString();
                    MyArray1[8] = DatosLineas.Tables[0].Rows[i]["CdadEntregar"].ToString();
                    MyArray1[9] = DatosLineas.Tables[0].Rows[i]["NumLinAbonar"].ToString();
                    MyArray1[10] = DatosLineas.Tables[0].Rows[i]["NumPack"].ToString();
                    MyArray1[11] = DatosLineas.Tables[0].Rows[i]["SecPack"].ToString();
                    MyArray1[12] = DatosLineas.Tables[0].Rows[i]["DtoCol"].ToString();
                    MyArray1[13] = DatosLineas.Tables[0].Rows[i]["ImpDtoCol"].ToString();
                    MyArray1[14] = DatosLineas.Tables[0].Rows[i]["NumLinea"].ToString();
                    MyArray1[15] = DatosLineas.Tables[0].Rows[i]["Variante"].ToString();
                    MyArray1[16] = DatosLineas.Tables[0].Rows[i]["CodVend"].ToString();

                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Crear2()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DE LA FACTURA
            try
            {

                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma = new NavisionDB.NavisionDBNas();
                        NasMultiForma.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma.ExecuteFunction = "PSTDMultiForma";
                        string[] MyArray1 = new string[25];
                        MyArray1[0] = "Devolucion";
                        MyArray1[1] = NumeroSerieNuevo;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][1].ToString(); // Cod.Forma Pago
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][2].ToString(); // Importe
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][3].ToString(); // Cambio
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][4].ToString(); // Nº Vale
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][5].ToString(); // Tipo de Pago
                        MyArray1[8] = CodTPV;
                        MyArray1[9] = CodTienda;
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][6].ToString(); // Cobrado
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][7].ToString(); // Nº Vale Reserva
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][8].ToString(); // Nº cheque regalo
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][9].ToString(); // Tipo de vale

                        //BYL: Se elimina PSTDMultiForma2 
                        MyArray1[14] = MultiForma.Tables[0].Rows[i][10].ToString();// Nº tarjeta dto.
                        MyArray1[15] = MultiForma.Tables[0].Rows[i][11].ToString();// Financiera
                        MyArray1[16] = MultiForma.Tables[0].Rows[i][12].ToString();// Nº autorización financiera
                        MyArray1[17] = MultiForma.Tables[0].Rows[i][13].ToString();// "Gastos financiación"
                        MyArray1[18] = MultiForma.Tables[0].Rows[i][14].ToString();// % Gastos financiación
                        MyArray1[19] = MultiForma.Tables[0].Rows[i][15].ToString();// Cliente que financia
                        MyArray1[20] = MultiForma.Tables[0].Rows[i][16].ToString();// Forma pago gastos financieros
                        MyArray1[21] = MultiForma.Tables[0].Rows[i][17].ToString();// Cta. contrapartida gts. finan.
                        MyArray1[22] = MultiForma.Tables[0].Rows[i][18].ToString();// Nº tarjeta de cobro
                        MyArray1[23] = MultiForma.Tables[0].Rows[i][19].ToString();// Fecha registro
                        MyArray1[24] = MultiForma.Tables[0].Rows[i][20].ToString();// Nº tarjeta cobro gastos financieros
                        //BYL: Se elimina PSTDMultiForma2 

                        NasMultiForma.Parameters = MyArray1;
                        NasMultiForma.SendParamsAsync(NasMultiForma, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Crear2()", "Error al insertar multiformas: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }


            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DEL DOCUMENTO
            //CON EL RESTO DE INFORMACION QUE FALTA
            /* ### BYL: Se elimina PSTDMultiForma2  ###
            try
            {

                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma2 = new NavisionDB.NavisionDBNas();
                        NasMultiForma2.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma2.ExecuteFunction = "PSTDMultiForma2";
                        string[] MyArray1 = new string[14];
                        MyArray1[0] = "Devolucion";
                        MyArray1[1] = NumeroSerieNuevo;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][10].ToString();// Nº tarjeta dto.
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][11].ToString();// Financiera
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][12].ToString();// Nº autorización financiera
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][13].ToString();// "Gastos financiación"
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][14].ToString();// % Gastos financiación
                        MyArray1[8] = MultiForma.Tables[0].Rows[i][15].ToString();// Cliente que financia
                        MyArray1[9] = MultiForma.Tables[0].Rows[i][16].ToString();// Forma pago gastos financieros
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][17].ToString();// Cta. contrapartida gts. finan.
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][18].ToString();// Nº tarjeta de cobro
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][19].ToString();// Fecha registro
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][20].ToString();// Nº tarjeta cobro gastos financieros                        

                        NasMultiForma2.Parameters = MyArray1;
                        NasMultiForma2.SendParamsAsync(NasMultiForma2, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Crear2()", "Error al insertar multiformas2: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }
            */

            //LLAMAMOS A LA FUNCION DE RECALCULAR LINEAS PARA LAS LINEAS QUE CONTIENEN PACK
            /* ### BYL: Se elimina ActualizaPreciosVenta  ###
            try
            {
                NavisionDB.NavisionDBNas NasRecalcularLineas = new NavisionDB.NavisionDBNas();
                NasRecalcularLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasRecalcularLineas.ExecuteFunction = "ActualizaPreciosVenta";
                string[] MyArray1 = new string[2];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = "3";
                NasRecalcularLineas.Parameters = MyArray1;
                NasRecalcularLineas.SendParamsAsync(NasRecalcularLineas, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Crear2()", "Error al recalcular línea factura: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }
            */
            //LLAMAMOS A LA FUNCION FACTURAR
            try
            {

                NavisionDB.NavisionDBNas NasFacturar = new NavisionDB.NavisionDBNas();
                NasFacturar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasFacturar.ExecuteFunction = "PSTDAbonar2";
                string[] MyArray1 = new string[1];
                MyArray1[0] = NumeroSerieNuevo;
                NasFacturar.Parameters = MyArray1;
                NasFacturar.SendParamsAsync(NasFacturar, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Crear2()", "Error al registrar abono: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE CREAR LA INFORMACION DE LA TABLA TICKET
            try
            {

                NavisionDB.NavisionDBNas NasTicket = new NavisionDB.NavisionDBNas();
                NasTicket.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasTicket.ExecuteFunction = "PSTDInfoTicketAbono";
                string[] MyArray1 = new string[10];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = CodTienda;
                MyArray1[2] = CodTPV;
                MyArray1[3] = FechaTicket;
                MyArray1[4] = HoraTicket;
                MyArray1[5] = ImporteVenta;
                MyArray1[6] = TotalaPagar;
                MyArray1[7] = "0";
                MyArray1[8] = CodVendedor;
                MyArray1[9] = Cliente;

                NasTicket.Parameters = MyArray1;
                NasTicket.SendParamsAsync(NasTicket, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Crear2()", "Error al insertar tabla ticket en abono: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet Entregas_Crear(string NumeroTicket,
                                string Cliente,
                                string CodTienda,
                                string CodDirEnvio,
                                DataSet DatosLineas,
                                string EnvioNombre,
                                string EnvioDireccion,
                                string EnvioCP,
                                string EnvioPoblacion,
                                string EnvioProvincia,
                                string EnvioAtencion,
                                string CodFormaPago,
                                string CodVendedor,
                                string FechaTicket,
                                string CodTPV,
                                string HoraTicket,
                                string TotalaPagar,
                                string ImporteVenta,
                                string ImporteEntregado,
                                string NumeroReserva,
                                string ImportePago,
                                string CtaContrapartida,
                                string CodAlmacen)
        //DataSet MultiForma)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "EntregasCrear()", "ERROR: No se ha validado, debe abrir login");

            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumeroTicket;


            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[16];
                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = Cliente;
                MyArray[2] = CodDirEnvio;
                MyArray[3] = EnvioNombre;
                MyArray[4] = EnvioDireccion;
                MyArray[5] = EnvioCP;
                MyArray[6] = EnvioPoblacion;
                MyArray[7] = EnvioProvincia;
                MyArray[8] = EnvioAtencion;
                MyArray[9] = CodTienda;
                MyArray[10] = CodFormaPago;
                MyArray[11] = CodVendedor;
                MyArray[12] = FechaTicket;
                MyArray[13] = NumeroReserva;
                MyArray[14] = ImporteEntregado;
                MyArray[15] = CodAlmacen;

                nas.ExecuteFunction = "PSTDEntregaGenerar";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);


            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Entregas_Crear()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTDGrabarLineaEntrega";

                    string[] MyArray1 = new string[7];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Quantity"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["UnitPrice"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["LineDiscount"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Description"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["NumLinea"].ToString();
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Entregas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }

            try
            {
                NavisionDB.NavisionDBNas NasFacturar = new NavisionDB.NavisionDBNas();
                NasFacturar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasFacturar.ExecuteFunction = "PSTDFacEntrega";
                string[] MyArray1 = new string[4];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = NumeroReserva;
                MyArray1[2] = ImportePago;
                MyArray1[3] = CtaContrapartida;
                NasFacturar.Parameters = MyArray1;
                NasFacturar.SendParamsAsync(NasFacturar, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Entregas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }

            try
            {
                NavisionDB.NavisionDBNas NasTicket = new NavisionDB.NavisionDBNas();
                NasTicket.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasTicket.ExecuteFunction = "PSTDInfoTicketFactura";
                string[] MyArray1 = new string[10];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = CodTienda;
                MyArray1[2] = CodTPV;
                MyArray1[3] = FechaTicket;
                MyArray1[4] = HoraTicket;
                MyArray1[5] = ImporteVenta;
                MyArray1[6] = TotalaPagar;
                MyArray1[7] = ImporteEntregado;
                MyArray1[8] = CodVendedor;
                MyArray1[9] = Cliente;

                NasTicket.Parameters = MyArray1;
                NasTicket.SendParamsAsync(NasTicket, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Entregas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet Reg_Transferencia(string NumeroTransferencia, string Vendedor, string Cliente,
                                         string NombreCli, string MotivoDev, string Comentario,
                                         string NLinea, DataSet DatosLineas, DataSet DatosMulti)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Reg_Transferencia()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                int i;
                if (DatosMulti.Tables.Count > 0)
                {
                    for (i = 0; i < DatosMulti.Tables[0].Rows.Count; i++)
                    {
                        NavisionDB.NavisionDBNas NasLinRec = new NavisionDB.NavisionDBNas();
                        NasLinRec.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasLinRec.ExecuteFunction = "PSTD_GrabarLinRecibidas";
                        string[] MyArray1 = new string[3];
                        MyArray1[0] = NumeroTransferencia;
                        MyArray1[1] = DatosMulti.Tables[0].Rows[i]["Cantidad"].ToString();
                        MyArray1[2] = DatosMulti.Tables[0].Rows[i]["NumLin"].ToString();

                        NasLinRec.Parameters = MyArray1;
                        NasLinRec.SendParamsAsync(NasLinRec, "", false);

                    }
                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reg_Transferencia()", "Error al insertar línea ped. transferencia: " + ex.Message);
                return DsResult;
            }



            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[7];

                MyArray[0] = NumeroTransferencia;
                MyArray[1] = Vendedor;
                MyArray[2] = Cliente;
                MyArray[3] = NombreCli;
                MyArray[4] = MotivoDev;
                MyArray[5] = Comentario;
                MyArray[6] = NLinea;
                nas.ExecuteFunction = "PSTD_Reg_Transferencia";
                nas.Parameters = MyArray;
                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                //this.BorrarCabecera(NumeroTransferencia, "Factura");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reg_Transferencia()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroTransferencia);
                return DsResult;
            }


            try
            {
                int i;
                if (DatosLineas.Tables.Count > 0)
                {
                    for (i = 0; i < DatosLineas.Tables[0].Rows.Count; i++)
                    {
                        NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                        NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasLineas.ExecuteFunction = "PSTD_Grabar_LinIncidencia";
                        string[] MyArray1 = new string[6];
                        MyArray1[0] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                        MyArray1[1] = DatosLineas.Tables[0].Rows[i]["LineNo"].ToString();
                        MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Cantidad"].ToString();
                        MyArray1[3] = DatosLineas.Tables[0].Rows[i]["Fecha"].ToString();
                        MyArray1[4] = DatosLineas.Tables[0].Rows[i]["Descri"].ToString();
                        MyArray1[5] = DatosLineas.Tables[0].Rows[i]["TipoInc"].ToString();
                        NasLineas.Parameters = MyArray1;
                        NasLineas.SendParamsAsync(NasLineas, "", false);

                    }
                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reg_Transferencia()", "Error al insertar línea: " + ex.Message);
                //this.BorrarLineas(NumeroTransferencia, "Factura");
                //this.BorrarCabecera(NumeroTransferencia, "Factura");
                return DsResult;
            }


            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroTransferencia;
            return DsResult;
        }

        public DataSet Abonos_Crear(string NumeroTicket,
                                            string Cliente,
                                            string CodTienda,
                                            string CodDirEnvio,
                                            DataSet DatosLineas,
                                            string EnvioNombre,
                                            string EnvioDireccion,
                                            string EnvioCP,
                                            string EnvioPoblacion,
                                            string EnvioProvincia,
                                            string EnvioAtencion,
                                            string CodFormaPago,
                                            string CodVendedor,
                                            string FechaTicket,
                                            string CodTPV,
                                            string HoraTicket,
                                            string ImporteDevolucion,
                                            string ImporteEntregado,
                                            string NumTicketAbonado,
                                            string CrearEntrega,
                                            string EnvioObserv)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Abonos_Crear()", "ERROR: No se ha validado, debe abrir login");

            //NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            //NavisionDBAdapter Da = new NavisionDBAdapter();
            //NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            //NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            //NavisionDBDataReader Rd = new NavisionDBDataReader();
            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumeroTicket;

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[16];
                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = Cliente;
                MyArray[2] = CodDirEnvio;
                MyArray[3] = EnvioNombre;
                MyArray[4] = EnvioDireccion;
                MyArray[5] = EnvioCP;
                MyArray[6] = EnvioPoblacion;
                MyArray[7] = EnvioProvincia;
                MyArray[8] = EnvioAtencion;
                MyArray[9] = CodTienda;
                MyArray[10] = CodFormaPago;
                MyArray[11] = CodVendedor;
                MyArray[12] = FechaTicket;
                MyArray[13] = NumTicketAbonado;
                MyArray[14] = CrearEntrega;
                MyArray[15] = EnvioObserv;

                nas.ExecuteFunction = "PSTDGrabarCabeceraAbono";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abonos_Crear()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTDGrabarLineaAbono";
                    string[] MyArray1 = new string[8];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Quantity"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["UnitPrice"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["LineDiscount"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["NumLinAbono"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["CrearEntrega"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["CdadEntregar"].ToString();
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abonos_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }

            try
            {
                NavisionDB.NavisionDBNas NasFacturar = new NavisionDB.NavisionDBNas();
                NasFacturar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasFacturar.ExecuteFunction = "PSTDAbonar";
                string[] MyArray1 = new string[1];
                MyArray1[0] = NumeroSerieNuevo;
                NasFacturar.Parameters = MyArray1;
                NasFacturar.SendParamsAsync(NasFacturar, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abonos_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }

            try
            {
                NavisionDB.NavisionDBNas NasTicket = new NavisionDB.NavisionDBNas();
                NasTicket.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasTicket.ExecuteFunction = "PSTDInfoTicketAbono";
                string[] MyArray1 = new string[9];

                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = CodTienda;
                MyArray1[2] = CodTPV;
                MyArray1[3] = FechaTicket;
                MyArray1[4] = HoraTicket;
                MyArray1[5] = ImporteDevolucion;
                MyArray1[6] = ImporteDevolucion;
                MyArray1[7] = CodVendedor;
                MyArray1[8] = Cliente;

                NasTicket.Parameters = MyArray1;
                NasTicket.SendParamsAsync(NasTicket, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Abonos_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Abono");
                this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet Subir_JefesSecc(string CodTienda,
                                       string vendedor,
                                       string hora,
                                       string fecha,
                                       string TiendaAbierta,
                                       DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Subir_JefesSecc()", "ERROR: No se ha validado, debe abrir login");


            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[4];
                MyArray[0] = CodTienda;
                MyArray[1] = fecha;
                MyArray[2] = vendedor;
                MyArray[3] = hora;

                nas.ExecuteFunction = "Registrar_Apertura_Tienda";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Subir_JefesSecc()", "Error al insertar la apertura tienda: " + ex.Message + ". Cabecera borrada: " + CodTienda);
                return DsResult;
            }

            //INTRODUCIMOS LOS JEFES DE SECCION
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "Registrar_Responsables_tienda";
                    string[] MyArray1 = new string[4];
                    MyArray1[0] = DatosLineas.Tables[0].Rows[i]["CodTienda"].ToString();
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["Fecha"].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["CodVendedor"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["CodGrupo"].ToString();
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Subir_JefesSecc()", "Error apertura tienda: " + ex.Message);
                //this.BorrarLineas(NumeroSerieNuevo, "Abono");
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }


            if (TiendaAbierta == "No")
            {

                try
                {
                    NavisionDB.NavisionDBNas NasFacturar = new NavisionDB.NavisionDBNas();
                    NasFacturar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasFacturar.ExecuteFunction = "Sincronizacion_general_apertur";
                    string[] MyArray1 = new string[2];
                    MyArray1[0] = CodTienda;
                    MyArray1[1] = fecha;
                    NasFacturar.Parameters = MyArray1;
                    NasFacturar.SendParamsAsync(NasFacturar, "", false);
                }
                catch (System.Messaging.MessageQueueException ex)
                {
                    DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Subir_JefesSecc()", "Error apertura tienda: " + ex.Message);
                    //this.BorrarLineas(NumeroSerieNuevo, "Abono");
                    //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                    return DsResult;
                }
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = CodTienda;
            return DsResult;
        }

        public DataSet Subir_ArqueoCaja(string CabCodArq, string CabCodTienda,
                                        string CabCodTPV, string CabFecha,
                                        string CabNumTurno, string CabHora,
                                        string CabCodVendedor, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Subir_ArqueoCaja()", "ERROR: No se ha validado, debe abrir login");


            //INTRODUCIMOS LA CABECERA DEL ARQUEO
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[7];
                MyArray[0] = CabCodArq;
                MyArray[1] = CabCodTienda;
                MyArray[2] = CabCodTPV;
                MyArray[3] = CabFecha;
                MyArray[4] = CabNumTurno;
                MyArray[5] = CabHora;
                MyArray[6] = CabCodVendedor;

                nas.ExecuteFunction = "RegistrarCabArqueo";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Subir_ArqueoCaja()", "Error cab. arqueo tpv: " + ex.Message + ". Cabecera borrada: " + CabCodArq);
                return DsResult;
            }

            //INTRODUCIMOS LAS LINEAS DEL ARQUEO
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "RegistrarLinArqueo";
                    string[] MyArray1 = new string[6];
                    MyArray1[0] = DatosLineas.Tables[0].Rows[i]["CodArqueo"].ToString();
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["Numlin"].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["FormaPago"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["ImporteReal"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["ImporteArq"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Diferencia"].ToString();
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Subir_ArqueoCaja()", "Error línea arqueo tpv: " + ex.Message);
                //this.BorrarLineas(NumeroSerieNuevo, "Abono");
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = CabCodArq;
            return DsResult;
        }

        public DataSet TarifaSolicitud(string Codigo, string CabCodTienda,
                                string CabCodTPV, string FechaCre,
                                string HoraCre, string Vendedor,
                                string Etiqueta, string FIni, string FFin,
                                string Tarifa, string ActCostes, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "TarifaSolicitud()", "ERROR: No se ha validado, debe abrir login");


            //INTRODUCIMOS LA CABECERA DE LA MOD. TARIFA
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[11];
                MyArray[0] = Codigo;
                MyArray[1] = CabCodTienda;
                MyArray[2] = CabCodTPV;
                MyArray[3] = FechaCre;
                MyArray[4] = HoraCre;
                MyArray[5] = Vendedor;
                MyArray[6] = Etiqueta;
                MyArray[7] = FIni;
                MyArray[8] = FFin;
                MyArray[9] = Tarifa;
                MyArray[10] = ActCostes;

                nas.ExecuteFunction = "PSTD_CabTarifaGenerar";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "TarifaSolicitud()", "Error cab. tarifa tpv: " + ex.Message + ". Cabecera borrada: " + Codigo);
                return DsResult;
            }

            //INTRODUCIMOS LAS LINEAS DEL ARQUEO
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_LinTarifaGenerar";
                    string[] MyArray1 = new string[16];
                    MyArray1[0] = Codigo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["Numlin"].ToString();
                    //MyArray1[2] = DatosLineas.Tables[0].Rows[i]["FechaReg"].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Hora"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["Etiqueta"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["CodProd"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["PreVenta"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["Coste"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["FIni"].ToString();
                    MyArray1[8] = DatosLineas.Tables[0].Rows[i]["FFin"].ToString();
                    MyArray1[9] = DatosLineas.Tables[0].Rows[i]["Motivo"].ToString();
                    MyArray1[10] = DatosLineas.Tables[0].Rows[i]["MrgActual"].ToString();
                    MyArray1[11] = DatosLineas.Tables[0].Rows[i]["UltPrecio"].ToString();
                    MyArray1[12] = DatosLineas.Tables[0].Rows[i]["PctCambio"].ToString();
                    MyArray1[13] = DatosLineas.Tables[0].Rows[i]["MrgCambio"].ToString();
                    MyArray1[14] = DatosLineas.Tables[0].Rows[i]["PctActual"].ToString();
                    MyArray1[15] = DatosLineas.Tables[0].Rows[i]["UMB"].ToString();

                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "TarifaSolicitud()", "Error línea tarifa tpv: " + ex.Message);
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = Codigo;
            return DsResult;
        }

        public DataSet TarifaAdmon(string Codigo, string VenAdmon,
                                   string Estado, string FAdmon,
                                   string ValDir, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "TarifaAdmon()", "ERROR: No se ha validado, debe abrir login");


            //INTRODUCIMOS LA CABECERA DE LA MOD. TARIFA
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[5];
                MyArray[0] = Codigo;
                MyArray[1] = VenAdmon;
                MyArray[2] = Estado;
                MyArray[3] = FAdmon;
                MyArray[4] = ValDir;

                nas.ExecuteFunction = "PSTD_CabTarifaAdmon";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "TarifaAdmon()", "Error cab. tarifa tpv: " + ex.Message + ". Cabecera borrada: " + Codigo);
                return DsResult;
            }

            //INTRODUCIMOS LAS LINEAS DE LA MODIFICACION DE TARIFA
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_LinTarifaAdmon";
                    string[] MyArray1 = new string[8];
                    MyArray1[0] = Codigo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["Numlin"].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["VenAdmon"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["Estado"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["FAdmon"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["ResAdmon"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["ObsAdmon"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["Bloqueo"].ToString();

                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "TarifaAdmon()", "Error línea tarifa tpv: " + ex.Message);
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = Codigo;
            return DsResult;
        }


        public DataSet TarifaDireccion(string Codigo, string VenDir,
                                       string Estado, string FDir,
                                       string ValDir, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "TarifaDireccion()", "ERROR: No se ha validado, debe abrir login");


            //INTRODUCIMOS LA CABECERA DE LA MOD. TARIFA
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[5];
                MyArray[0] = Codigo;
                MyArray[1] = VenDir;
                MyArray[2] = Estado;
                MyArray[3] = FDir;
                MyArray[4] = ValDir;

                nas.ExecuteFunction = "PSTD_CabTarifaDireccion";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "TarifaAdmon()", "Error cab. tarifa tpv: " + ex.Message + ". Cabecera borrada: " + Codigo);
                return DsResult;
            }

            //INTRODUCIMOS LAS LINEAS DE LA MODIFICACION DE TARIFA
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_LinTarifaDireccion";
                    string[] MyArray1 = new string[8];
                    MyArray1[0] = Codigo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["Numlin"].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["VenDir"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["Estado"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["FDir"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["ResDir"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["ObsDir"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["Bloqueo"].ToString();

                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "TarifaDireccion()", "Error línea tarifa tpv: " + ex.Message);
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = Codigo;
            return DsResult;
        }

        public DataSet Registrar_CierreTienda(string Tienda, string Fecha,
                                              string Vendedor, string Hora)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Registrar_CierreTienda()", "ERROR: No se ha validado, debe abrir login");


            //LLAMAMOS A LA FUNCION DE CERRAR TIENDA
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[4];
                MyArray[0] = Tienda;
                MyArray[1] = Fecha;
                MyArray[2] = Vendedor;
                MyArray[3] = Hora;

                nas.ExecuteFunction = "Registrar_Cierre_Tienda";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Registrar_CierreTienda()", "Error cierre tienda: " + ex.Message + ". Cierre tienda: " + Tienda);
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = Tienda;
            return DsResult;
        }


        public DataSet Registrar_Asistencia(string Tienda, string Fecha,
                                            string Vendedor, string Hora, string GrupoVenta)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Registrar_Asistencia()", "ERROR: No se ha validado, debe abrir login");


            //LLAMAMOS A LA FUNCION DE CONTROL DE ASISTENCIA
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[5];
                MyArray[0] = Tienda;
                MyArray[1] = Fecha;
                MyArray[2] = Vendedor;
                MyArray[3] = Hora;
                MyArray[4] = GrupoVenta;

                nas.ExecuteFunction = "Registrar_Asistencia";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                //this.BorrarCabecera(NumeroSerieNuevo, "Abono");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Registrar_Asistencia()", "Error cierre tienda: " + ex.Message + ". Cierre tienda: " + Tienda);
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = Tienda;
            return DsResult;
        }

        public DataSet Reservas_Crear(string NumeroTicket, string Cliente, string CodTienda,
                                      string CodTPV, string EnvioNombre, string EnvioDireccion, string EnvioCP,
                                      string EnvioPoblacion, string EnvioProvincia, string EnvioAtencion,
                                      string CodFormaPago, string CodVendedor, string FechaTicket,
                                      string CrearEntrega, string AplicarDto, string NumTarjetaDto,
                                      string GruDtoCol, string NumFidelizacion, string CobroTrans,
                                      string FactAuto, string RepInmediata, string DesColectivo,
                                      string AlmDestino, string EnvioObserv, string NumLinPago,
                                      string CodVenAnticipo, string FechaEnvio,
                                      DataSet DatosLineas, DataSet MultiForma)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Reservas_Crear()", "ERROR: No se ha validado, debe abrir login");

            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumeroTicket;

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[24];
                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = Cliente;
                MyArray[2] = CodTienda;
                MyArray[3] = CodFormaPago;
                MyArray[4] = CodVendedor;
                MyArray[5] = FechaTicket;
                MyArray[6] = CrearEntrega;
                MyArray[7] = AplicarDto;
                MyArray[8] = NumTarjetaDto;
                MyArray[9] = GruDtoCol;
                MyArray[10] = NumFidelizacion;
                MyArray[11] = CobroTrans;
                MyArray[12] = FactAuto;
                MyArray[13] = RepInmediata;
                MyArray[14] = DesColectivo;
                MyArray[15] = AlmDestino;

                //BYL: Se elimina PSTDInformacionEnvio
                MyArray[16] = EnvioNombre;
                MyArray[17] = EnvioDireccion;
                MyArray[18] = EnvioCP;
                MyArray[19] = EnvioPoblacion;
                MyArray[20] = EnvioProvincia;
                MyArray[21] = EnvioAtencion;
                MyArray[22] = EnvioObserv;
                MyArray[23] = FechaEnvio;
                //BYL: Se elimina PSTDInformacionEnvio

                nas.ExecuteFunction = "PSTDGrabarCabeceraReserva";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Crear()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LOS DATOS DE ENVIO O RECOGIDA EN CASO DE ABONOS
            /* BYL: Se elimina PSTDInformacionEnvio
            try
            {
                NavisionDB.NavisionDBNas NasDatosEnvio = new NavisionDB.NavisionDBNas();
                NasDatosEnvio.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasDatosEnvio.ExecuteFunction = "PSTDInformacionEnvio";
                string[] MyArray1 = new string[11];
                MyArray1[0] = NumeroSerieNuevo;
                MyArray1[1] = Cliente;
                MyArray1[2] = "Reserva";
                MyArray1[3] = EnvioNombre;
                MyArray1[4] = EnvioDireccion;
                MyArray1[5] = EnvioCP;
                MyArray1[6] = EnvioPoblacion;
                MyArray1[7] = EnvioProvincia;
                MyArray1[8] = EnvioAtencion;
                MyArray1[9] = EnvioObserv;
                MyArray1[10] = FechaEnvio;
                NasDatosEnvio.Parameters = MyArray1;
                NasDatosEnvio.SendParamsAsync(NasDatosEnvio, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Crear()", "Error al introducir datos envio: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }
            */

            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTDGrabarLineaReserva";
                    string[] MyArray1 = new string[16];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Quantity"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["UnitPrice"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["LineDiscount"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Description"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["Type"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["CrearEntrega"].ToString();
                    MyArray1[8] = DatosLineas.Tables[0].Rows[i]["CdadEntregar"].ToString();                    
                    MyArray1[9] = DatosLineas.Tables[0].Rows[i]["NumPack"].ToString();
                    MyArray1[10] = DatosLineas.Tables[0].Rows[i]["SecPack"].ToString();
                    MyArray1[11] = DatosLineas.Tables[0].Rows[i]["DtoCol"].ToString();
                    MyArray1[12] = DatosLineas.Tables[0].Rows[i]["ImpDtoCol"].ToString();
                    MyArray1[13] = DatosLineas.Tables[0].Rows[i]["NumLinea"].ToString();
                    MyArray1[14] = DatosLineas.Tables[0].Rows[i]["Cancelada"].ToString();
                    MyArray1[15] = DatosLineas.Tables[0].Rows[i]["Variante"].ToString();

                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DE LA FACTURA
            try
            {
                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma = new NavisionDB.NavisionDBNas();
                        NasMultiForma.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma.ExecuteFunction = "PSTDMultiForma";
                        string[] MyArray1 = new string[25];
                        MyArray1[0] = "Reserva";
                        MyArray1[1] = NumeroSerieNuevo;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][1].ToString(); // Cod.Forma Pago
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][2].ToString(); // Importe
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][3].ToString(); // Cambio
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][4].ToString(); // Nº Vale
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][5].ToString(); // Tipo de Pago
                        MyArray1[8] = CodTPV;
                        MyArray1[9] = CodTienda;
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][6].ToString(); // Cobrado
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][7].ToString(); // Nº Vale Reserva
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][8].ToString(); // Nº cheque regalo
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][9].ToString(); // Tipo de vale

                        //BYL: Se elimina PSTDMultiForma2 
                        MyArray1[14] = MultiForma.Tables[0].Rows[i][10].ToString();// Nº tarjeta dto.
                        MyArray1[15] = MultiForma.Tables[0].Rows[i][11].ToString();// Financiera
                        MyArray1[16] = MultiForma.Tables[0].Rows[i][12].ToString();// Nº autorización financiera
                        MyArray1[17] = MultiForma.Tables[0].Rows[i][13].ToString();// "Gastos financiación"
                        MyArray1[18] = MultiForma.Tables[0].Rows[i][14].ToString();// % Gastos financiación
                        MyArray1[19] = MultiForma.Tables[0].Rows[i][15].ToString();// Cliente que financia
                        MyArray1[20] = MultiForma.Tables[0].Rows[i][16].ToString();// Forma pago gastos financieros
                        MyArray1[21] = MultiForma.Tables[0].Rows[i][17].ToString();// Cta. contrapartida gts. finan.
                        MyArray1[22] = MultiForma.Tables[0].Rows[i][18].ToString();// Nº tarjeta de cobro
                        MyArray1[23] = MultiForma.Tables[0].Rows[i][19].ToString();// Fecha registro
                        MyArray1[24] = MultiForma.Tables[0].Rows[i][20].ToString();// Nº tarjeta cobro gastos financieros
                        //BYL: Se elimina PSTDMultiForma2 

                        NasMultiForma.Parameters = MyArray1;
                        NasMultiForma.SendParamsAsync(NasMultiForma, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Crear()", "Error al insertar multiformas: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }


            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DEL DOCUMENTO
            //CON EL RESTO DE INFORMACION QUE FALTA
            /* ### BYL: Se elimina PSTDMultiForma2  ###
            try
            {
                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma2 = new NavisionDB.NavisionDBNas();
                        NasMultiForma2.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma2.ExecuteFunction = "PSTDMultiForma2";
                        string[] MyArray1 = new string[14];
                        MyArray1[0] = "Reserva";
                        MyArray1[1] = NumeroSerieNuevo;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][10].ToString();// Nº tarjeta dto.
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][11].ToString();// Financiera
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][12].ToString();// Nº autorización financiera
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][13].ToString();// "Gastos financiación"
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][14].ToString();// % Gastos financiación
                        MyArray1[8] = MultiForma.Tables[0].Rows[i][15].ToString();// Cliente que financia
                        MyArray1[9] = MultiForma.Tables[0].Rows[i][16].ToString();// Forma pago gastos financieros
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][17].ToString();// Cta. contrapartida gts. finan.
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][18].ToString();// Nº tarjeta de cobro
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][19].ToString();// Fecha registro
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][20].ToString();// Nº tarjeta cobro gastos financieros

                        NasMultiForma2.Parameters = MyArray1;
                        NasMultiForma2.SendParamsAsync(NasMultiForma2, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Crear()", "Error al insertar multiformas2: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }
            */

            //LANZAMOS LA RESERVA SI PROCEDE
            try
            {
                NavisionDB.NavisionDBNas nasLanzar = new NavisionDB.NavisionDBNas();
                nasLanzar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray2 = new string[1];
                MyArray2[0] = NumeroSerieNuevo;

                nasLanzar.ExecuteFunction = "PSTD_Lanzar_Reserva";
                nasLanzar.Parameters = MyArray2;

                nasLanzar.SendParamsAsync(nasLanzar, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            //registramos el anticipo
            //if numlinpago <> 0 then hacer esto

            try
            {
                NavisionDB.NavisionDBNas nasAnticipo = new NavisionDB.NavisionDBNas();
                nasAnticipo.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray2 = new string[4];
                MyArray2[0] = NumeroSerieNuevo;
                MyArray2[1] = NumLinPago;
                MyArray2[2] = CodTienda;
                MyArray2[3] = CodVenAnticipo;

                nasAnticipo.ExecuteFunction = "PSTDReservaRegAnticipo";
                nasAnticipo.Parameters = MyArray2;

                nasAnticipo.SendParamsAsync(nasAnticipo, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Crear()", "Error al registrar el anticipo: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }


            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet PedCompra_Generar(string NumeroTicket,
                                    string Proveedor,
                                    string CodTienda,
                                    string EnvioNombre,
                                    string EnvioDireccion,
                                    string EnvioCP,
                                    string EnvioPoblacion,
                                    string EnvioProvincia,
                                    string EnvioAtencion,
                                    string CodFormaPago,
                                    string CodVendedor,
                                    string FechaTicket,
                                    DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "PedCompra_Generar()", "ERROR: No se ha validado, debe abrir login");

            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumeroTicket;

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[13];
                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = Proveedor;
                MyArray[2] = EnvioNombre;
                MyArray[3] = EnvioDireccion;
                MyArray[4] = EnvioCP;
                MyArray[5] = EnvioPoblacion;
                MyArray[6] = EnvioProvincia;
                MyArray[7] = EnvioAtencion;
                MyArray[8] = CodTienda;
                MyArray[9] = CodFormaPago;
                MyArray[10] = CodVendedor;
                MyArray[11] = FechaTicket;
                MyArray[12] = "";

                nas.ExecuteFunction = "PSTD_PedCompra_Act";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "PedCompra_Generar()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_GrabarLineaPedCompra";
                    string[] MyArray1 = new string[8];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Quantity"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["UnitPrice"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["LineDiscount"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Description"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["NumLinea"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["CdadRecibir"].ToString();
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);
                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "PedCompra_Generar()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            try
            {
                NavisionDB.NavisionDBNas nasLanzar = new NavisionDB.NavisionDBNas();
                nasLanzar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray2 = new string[1];
                MyArray2[0] = NumeroSerieNuevo;

                nasLanzar.ExecuteFunction = "PSTD_Lanzar_PedCompra";
                nasLanzar.Parameters = MyArray2;

                nasLanzar.SendParamsAsync(nasLanzar, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "PedCompra_Generar()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }


            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet PedCompra_Registrar(string NumeroTicket,
                            string Proveedor,
                            string CodTienda,
                            string EnvioNombre,
                            string EnvioDireccion,
                            string EnvioCP,
                            string EnvioPoblacion,
                            string EnvioProvincia,
                            string EnvioAtencion,
                            string CodFormaPago,
                            string CodVendedor,
                            string FechaTicket,
                            string NumAlbaran,
                            DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "PedCompra_Actualizar()", "ERROR: No se ha validado, debe abrir login");

            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumeroTicket;

            //
            //ACTUALIZAMOS EL PEDIDO DE COMPRA
            //
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[13];
                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = Proveedor;
                MyArray[2] = EnvioNombre;
                MyArray[3] = EnvioDireccion;
                MyArray[4] = EnvioCP;
                MyArray[5] = EnvioPoblacion;
                MyArray[6] = EnvioProvincia;
                MyArray[7] = EnvioAtencion;
                MyArray[8] = CodTienda;
                MyArray[9] = CodFormaPago;
                MyArray[10] = CodVendedor;
                MyArray[11] = FechaTicket;
                MyArray[12] = NumAlbaran;

                nas.ExecuteFunction = "PSTD_PedCompra_Act";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "PedCompra_Actualizar()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }

            //
            //ACTUALIZAMOS LAS LINEAS DEL PEDIDO CON LA CANTIDAD A RECIBIR
            //
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_Act_LineaPedCompra";
                    string[] MyArray1 = new string[8];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Quantity"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["UnitPrice"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["LineDiscount"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Description"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["NumLinea"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["CdadRecibir"].ToString();
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);
                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "PedCompra_Actualizar()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            //
            //REGISTRAMOS EL PEDIDO
            //
            try
            {
                NavisionDB.NavisionDBNas NasFacturar = new NavisionDB.NavisionDBNas();
                NasFacturar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                NasFacturar.ExecuteFunction = "PSTD_Registrar_PedCompra";
                string[] MyArray1 = new string[1];
                MyArray1[0] = NumeroSerieNuevo;
                NasFacturar.Parameters = MyArray1;
                NasFacturar.SendParamsAsync(NasFacturar, "", false);
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Factura");
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public bool BorrarCabecera(string Numero, string tipo)
        {
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter Da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 36;   //Dt.TableName = "Sales Header";
                Dt.AddFilter(1, tipo);   //Dt.AddFilter("Document Type", tipo);
                Dt.AddFilter(3, Numero); //Dt.AddFilter("No.", Numero);
                Da.AddTable(Dt);
                Da.Fill(ref ds, true);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                Da.DeleteItem = ds;
                try
                {
                    this.Connection.BWT();
                    Da.Update();
                    this.Connection.EWT();
                }
                catch (Exception excep)
                {
                    this.Connection.AWT();
                    throw new Exception("BorrarCabecera(2): " + excep.Message);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("BorrarCabecera: " + ex.Message);
            }
            return true;
        }

        public bool BorrarLineas(string Numero, string tipo)
        {
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter Da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 37;                                        //Dt.TableName = "Sales Line";
                Dt.AddFilter(1, tipo);                                                         //Dt.AddFilter("Document Type", tipo);
                Dt.AddFilter(3, Numero);                                 //Dt.AddFilter("Document No.", Numero);
                Da.AddTable(Dt);
                Da.Fill(ref ds, true);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                Da.DeleteItem = ds;
                try
                {
                    this.Connection.BWT();
                    Da.Update();
                    this.Connection.EWT();
                }
                catch (Exception excep)
                {
                    this.Connection.AWT();
                    throw new Exception("BorrarLineas(2): " + excep.Message);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("BorrarLineas: " + ex.Message);
            }
            return true;
        }

        public DataSet Cliente_Ficha_PorCodigo(string CodCliente)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cliente_Ficha_PorCodigo()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableNo = 18; //Cliente
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.AddColumn(5);
                //dt.AddColumn("Contact");
                dt.AddColumn(91);
                dt.AddColumn(7);
                dt.AddColumn(92);
                dt.AddColumn(9);
                dt.AddColumn(102);
                dt.AddColumn(86);
                //dt.AddColumn("OpcionReferenciaPedido");
                //dt.AddColumn("OpcionPersonaContacto");
                dt.AddColumn(47);
                dt.AddColumn(27);
                //DPA++
                dt.AddColumn(34);
                //DPA--
                dt.AddFilter(1, CodCliente);

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Cliente_Ficha_PorCodigo()", ex.Message);
            }
        }
        public DataSet Cliente_Ficha_NIF(string CIF)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cliente_Ficha_NIF()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableNo = 18; //Cliente
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.AddColumn(5);
                //dt.AddColumn("Contact");
                dt.AddColumn(91);
                dt.AddColumn(7);
                dt.AddColumn(92);
                dt.AddColumn(9);
                dt.AddColumn(102);
                dt.AddColumn(86);
                //dt.AddColumn("OpcionReferenciaPedido");
                //dt.AddColumn("OpcionPersonaContacto");
                dt.AddColumn(47);
                dt.AddColumn(27);
                //DPA++
                dt.AddColumn(34);
                //DPA--
                dt.AddFilter(86, CIF);
                dt.KeyInNavisionFormat = "VAT Registration No.";

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Cliente_Ficha_NIF()", ex.Message);
            }
        }


        public DataSet Cliente_Ficha_Tlfno(string Telefono)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cliente_Ficha_Tlfno()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableNo = 18; //Cliente
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.AddColumn(5);
                //dt.AddColumn("Contact");
                dt.AddColumn(91);
                dt.AddColumn(7);
                dt.AddColumn(92);
                dt.AddColumn(9);
                dt.AddColumn(102);
                dt.AddColumn(86);
                //dt.AddColumn("OpcionReferenciaPedido");
                //dt.AddColumn("OpcionPersonaContacto");
                dt.AddColumn(47);
                dt.AddColumn(27);
                //DPA++
                dt.AddColumn(34);
                //DPA--
                dt.AddFilter(9, Telefono);
                dt.KeyInNavisionFormat = "Phone No.";
                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Cliente_Ficha_Tlfno()", ex.Message);
            }
        }

        public DataSet Proveedor_Ficha_NIF(string CIF)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Proveedor_Ficha_NIF()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableNo = 23; //Proveedor
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.AddColumn(5);
                //dt.AddColumn("Contact");
                dt.AddColumn(91);
                dt.AddColumn(7);
                dt.AddColumn(92);
                dt.AddColumn(9);
                dt.AddColumn(102);
                dt.AddColumn(86);
                //dt.AddColumn("OpcionReferenciaPedido");
                //dt.AddColumn("OpcionPersonaContacto");
                dt.AddColumn(47);
                dt.AddColumn(27);
                dt.AddColumn(88);   //Grupo contable negocio
                dt.AddColumn(110);  //Grupo registro IVA neg.
                dt.AddColumn(21);   //Grupo contable proveedor

                dt.AddFilter(86, CIF);
                dt.KeyInNavisionFormat = "VAT Registration No.";

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Proveedor_Ficha_NIF()", ex.Message);
            }
        }

        public DataSet Producto_Stock(string NumeroProducto, DataSet DatosLineas)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Producto_Stock()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                int i = 0;
                DataSet DsResult = new DataSet();
                DataTable Tabla = new DataTable();
                Tabla.TableName = "Resultado";
                Tabla.Columns.Add("Almacen");
                Tabla.Columns.Add("Existencias");


                DsResult.Tables.Add(Tabla);
                DataRow Dr;

                if (DatosLineas.Tables[0].Rows.Count > 0)
                {
                    for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                    {
                        ds = new DataSet();
                        dt.Reset();
                        da = new NavisionDBAdapter();
                        dt.TableNo = 27; //Producto
                        dt.AddColumn(1);
                        dt.AddColumn(3);
                        dt.AddColumn(68);

                        dt.AddFilter(1, NumeroProducto);
                        dt.AddFilter(67, DatosLineas.Tables[0].Rows[i][0].ToString());

                        da.AddTable(dt);


                        da.Fill(ref ds, false);
                        Utilidades.CompletarDataSet(ref ds, false, false);
                        //Primer parametro false = string vacia o true = espacio en blanco
                        //Segundo parametro fechas true = no mete nada y false 01010001

                        if (ds.Tables[0].Rows.Count > 0)
                        {
                            Dr = Tabla.NewRow();
                            Dr[0] = DatosLineas.Tables[0].Rows[i][0].ToString();
                            Dr[1] = ds.Tables[0].Rows[0][2];
                            Tabla.Rows.Add(Dr);
                            Tabla.AcceptChanges();
                        }
                    }
                    DsResult.AcceptChanges();
                }
                return DsResult;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Producto_Stock()", ex.Message);
            }
        }

        public DataSet Producto_Stock_Variante(string NumeroProducto, DataSet DatosLineas,
                                               DataSet DatosMulti)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Producto_Stock_Variante()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                int i = 0;
                DataSet DsResult = new DataSet();
                DataTable Tabla = new DataTable();
                Tabla.TableName = "Resultado";
                Tabla.Columns.Add("Almacen");
                Tabla.Columns.Add("Variante");
                Tabla.Columns.Add("Existencias");


                DsResult.Tables.Add(Tabla);
                DataRow Dr;

                if (DatosLineas.Tables[0].Rows.Count > 0)
                {
                    for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                    {
                        for (int z = 0; z <= DatosMulti.Tables[0].Rows.Count - 1; z++)
                        {
                            ds = new DataSet();
                            dt.Reset();
                            da = new NavisionDBAdapter();
                            dt.TableNo = 27; //Producto
                            dt.AddColumn(1);
                            dt.AddColumn(3);
                            dt.AddColumn(68);

                            dt.AddFilter(1, NumeroProducto);
                            dt.AddFilter(67, DatosLineas.Tables[0].Rows[i][0].ToString());
                            dt.AddFilter(5424, DatosMulti.Tables[0].Rows[z][0].ToString());

                            da.AddTable(dt);

                            da.Fill(ref ds, false);
                            Utilidades.CompletarDataSet(ref ds, false, false);
                            //Primer parametro false = string vacia o true = espacio en blanco
                            //Segundo parametro fechas true = no mete nada y false 01010001

                            if (ds.Tables[0].Rows.Count > 0)
                            {
                                Dr = Tabla.NewRow();
                                Dr[0] = DatosLineas.Tables[0].Rows[i][0].ToString();
                                Dr[1] = DatosMulti.Tables[0].Rows[z][0].ToString();
                                Dr[2] = ds.Tables[0].Rows[0][2];
                                Tabla.Rows.Add(Dr);
                                Tabla.AcceptChanges();
                            }
                        }
                    }
                    DsResult.AcceptChanges();
                }
                return DsResult;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Producto_Stock_Variante()", ex.Message);
            }
        }

        public DataSet Cliente_Ficha_Nombre(string Nombre)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cliente_Ficha_Nombre()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableNo = 18; //Cliente
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.AddColumn(5);
                //dt.AddColumn("Contact");
                dt.AddColumn(91);
                dt.AddColumn(7);
                dt.AddColumn(92);
                dt.AddColumn(9);
                dt.AddColumn(102);
                dt.AddColumn(86);
                //dt.AddColumn("OpcionReferenciaPedido");
                //dt.AddColumn("OpcionPersonaContacto");
                dt.AddColumn(47);
                dt.AddColumn(27);
                //DPA++
                dt.AddColumn(34);
                //DPA--
                dt.AddFilter(2, "@*" + Nombre + "*");
                dt.KeyInNavisionFormat = "Name";

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Cliente_Ficha_Nombre()", ex.Message);
            }
        }

        public DataSet Proveedor_Ficha_Nombre(string Nombre)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Proveedor_Ficha_Nombre()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();


                dt.TableNo = 23; //Proveedor
                dt.AddColumn(1);
                dt.AddColumn(2);
                dt.AddColumn(5);
                dt.AddColumn(91);
                dt.AddColumn(7);
                dt.AddColumn(92);
                dt.AddColumn(9);
                dt.AddColumn(102);
                dt.AddColumn(86);
                dt.AddColumn(47);
                dt.AddColumn(27);
                dt.AddColumn(88);   //Grupo contable negocio
                dt.AddColumn(110);  //Grupo registro IVA neg.
                dt.AddColumn(21);   //Grupo contable proveedor

                dt.AddFilter(2, "@*" + Nombre + "*");
                dt.KeyInNavisionFormat = "Name";

                da.AddTable(dt);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Proveedor_Ficha_Nombre()", ex.Message);
            }
        }

        public DataSet Cliente_Contador_Nombre(string Nombre)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cliente_Contador_Nombre()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader Rd = new NavisionDBDataReader();

                DataSet ds = new DataSet();

                dt.TableNo = 18; //Cliente
                dt.AddColumn(2);
                dt.AddFilter(2, "@*" + Nombre + "*");
                dt.KeyInNavisionFormat = "Name";
                Cmd.Table = dt;
                Rd = Cmd.ExecuteReader(false);
                return Utilidades.GenerarResultado(Convert.ToString(Rd.RecordsAffected));

            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Cliente_Contador_Nombre()", ex.Message);
            }
        }

        public DataSet Proveedor_Contador_Nombre(string Nombre)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Proveedor_Contador_Nombre()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader Rd = new NavisionDBDataReader();

                DataSet ds = new DataSet();

                dt.TableNo = 23; //Proveedor
                dt.AddColumn(2);
                dt.AddFilter(2, "@*" + Nombre + "*");
                dt.KeyInNavisionFormat = "Name";
                Cmd.Table = dt;
                Rd = Cmd.ExecuteReader(false);
                return Utilidades.GenerarResultado(Convert.ToString(Rd.RecordsAffected));

            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Proveedor_Contador_Nombre()", ex.Message);
            }
        }

        public DataSet Cliente_Contador_Codigo(string Codigo)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cliente_Contador_Codigo()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader Rd = new NavisionDBDataReader();

                DataSet ds = new DataSet();

                dt.TableNo = 18; //Cliente
                dt.AddColumn(1);
                dt.AddFilter(1, Codigo);
                Cmd.Table = dt;
                Rd = Cmd.ExecuteReader(false);

                return Utilidades.GenerarResultado(Convert.ToString(Rd.RecordsAffected));

            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Cliente_Contador_Codigo()", ex.Message);
            }
        }
        public DataSet Cliente_Contador_NIF(string CIF)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cliente_Contador_NIF()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader Rd = new NavisionDBDataReader();

                DataSet ds = new DataSet();

                dt.TableNo = 18; //Cliente
                dt.AddColumn(86);
                dt.AddFilter(86, CIF);
                dt.KeyInNavisionFormat = "VAT Registration No.";
                Cmd.Table = dt;
                Rd = Cmd.ExecuteReader(false);
                return Utilidades.GenerarResultado(Convert.ToString(Rd.RecordsAffected));

            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Cliente_Contador_NIF()", ex.Message);
            }
        }

        public DataSet Cliente_Contador_Tlfno(string Telefono)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cliente_Contador_Tlfno()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader Rd = new NavisionDBDataReader();

                DataSet ds = new DataSet();

                dt.TableNo = 18; //Cliente
                dt.AddColumn(9);
                dt.AddFilter(9, Telefono);
                dt.KeyInNavisionFormat = "Phone No.";
                Cmd.Table = dt;
                Rd = Cmd.ExecuteReader(false);
                return Utilidades.GenerarResultado(Convert.ToString(Rd.RecordsAffected));

            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Cliente_Contador_Tlfno()", ex.Message);
            }
        }

        public DataSet Proveedor_Contador_NIF(string CIF)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Proveedor_Contador_NIF()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
                NavisionDBDataReader Rd = new NavisionDBDataReader();

                DataSet ds = new DataSet();

                dt.TableNo = 23; //Proveedor
                dt.AddColumn(86);
                dt.AddFilter(86, CIF);
                dt.KeyInNavisionFormat = "VAT Registration No.";
                Cmd.Table = dt;
                Rd = Cmd.ExecuteReader(false);
                return Utilidades.GenerarResultado(Convert.ToString(Rd.RecordsAffected));

            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Proveedor_Contador_NIF()", ex.Message);
            }
        }

        public DataSet AltaCliente(string numero, string Nombre, string Direccion, string CP, string Poblacion, string Provincia,
                                   string Telefono, string Telefono2, string Email, string CIF, string CodFormaPago,
                                   string CodVendedor, string CodTerminosPago, string GrupoContCliente,
                                   string GrupoContNegocio, string GrupoRegIVANeg, string Newsletter,
                                   string NombreDos, string ShopCode)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "AltaCliente()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[19];
                MyArray[0] = numero;
                MyArray[1] = Nombre;
                MyArray[2] = Direccion;
                MyArray[3] = CP;
                MyArray[4] = Poblacion;
                MyArray[5] = Provincia;
                MyArray[6] = Telefono;
                MyArray[7] = Telefono2;
                MyArray[8] = Email;
                MyArray[9] = CIF;
                MyArray[10] = CodFormaPago;
                MyArray[11] = CodVendedor;
                MyArray[12] = CodTerminosPago;
                MyArray[13] = GrupoContCliente;
                MyArray[14] = GrupoContNegocio;
                MyArray[15] = GrupoRegIVANeg;
                MyArray[16] = Newsletter;
                MyArray[17] = NombreDos;
                MyArray[18] = ShopCode;

                nas.ExecuteFunction = "PSTDAltaCliente";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "AltaCliente()", "Error al insertar Cliente: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }

        public DataSet Mod_Cliente_Factura(string numero, string Nombre, string Nombre2,
                                           string Direccion, string CP, string Poblacion,
                                           string Provincia, string CIF, string NumFactura)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Mod_Cliente_Factura()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[9];
                MyArray[0] = NumFactura;
                MyArray[1] = numero;
                MyArray[2] = Nombre;
                MyArray[3] = Nombre2;
                MyArray[4] = Direccion;
                MyArray[5] = CP;
                MyArray[6] = Poblacion;
                MyArray[7] = Provincia;
                MyArray[8] = CIF;

                nas.ExecuteFunction = "PSTDMod_Cliente_Factura";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Mod_Cliente_Factura()", "Error al modificar cliente factura: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }


        public DataSet AltaTarifa(string CodProducto, string TipoVenta, string CodVenta,
                                  string FechaIni, string Divisa, string Variante, string UDS,
                                  string CdadMin, string PreVenta, string IvaInc,
                                  string DtoFactura, string IvaNegocio, string FechaFin,
                                  string DtoLinea, string FechaMod)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "AltaTarifa()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[15];
                MyArray[0] = CodProducto;
                MyArray[1] = TipoVenta;
                MyArray[2] = CodVenta;
                MyArray[3] = FechaIni;
                MyArray[4] = Divisa;
                MyArray[5] = Variante;
                MyArray[6] = UDS;
                MyArray[7] = CdadMin;
                MyArray[8] = PreVenta;
                MyArray[9] = IvaInc;
                MyArray[10] = DtoFactura;
                MyArray[11] = IvaNegocio;
                MyArray[12] = FechaFin;
                MyArray[13] = DtoLinea;
                MyArray[14] = FechaMod;

                nas.ExecuteFunction = "PSTDAltaTarifa";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "AltaTarifa()", "Error al insertar Cliente: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }

        public DataSet HistCambioTarifa(string CodProducto, string UDS, string CodTarifa,
                          string PreVenta, string Coste, string FechaIni, string FechaFin,
                          string VendSol, string VendResol, string Motivo,
                          string MargenAct, string ImpAct, string UltPrecVenta,
                          string FechaSol, string MargenSol, string ImpSol, string FechaResol,
                          string Estado, string Resolucion, string CodTienda, string CodTpv)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "HistCambioTarifa()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[21];
                MyArray[0] = CodProducto;
                MyArray[1] = UDS;
                MyArray[2] = CodTarifa;
                MyArray[3] = PreVenta;
                MyArray[4] = Coste;
                MyArray[5] = FechaIni;
                MyArray[6] = FechaFin;
                MyArray[7] = VendSol;
                MyArray[8] = VendResol;
                MyArray[9] = Motivo;
                MyArray[10] = MargenAct;
                MyArray[11] = ImpAct;
                MyArray[12] = UltPrecVenta;
                MyArray[13] = FechaSol;
                MyArray[14] = MargenSol;
                MyArray[15] = ImpSol;
                MyArray[16] = FechaResol;
                MyArray[17] = Estado;
                MyArray[18] = Resolucion;
                MyArray[19] = CodTienda;
                MyArray[20] = CodTpv;

                nas.ExecuteFunction = "PSTDHistCambioTarifa";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "HistCambioTarifa()", "Error al insertar Cliente: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }

        public DataSet Apertura_Tienda_TPV(string CodTienda, string CodTPV, string FechaIni,
                  string HoraIni, string SaldoIni, string Turno, string Apertura,
                  string Vendedor, string SincrEmp, string Activo)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Apertura_Tienda_TPV()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[10];
                MyArray[0] = CodTienda;
                MyArray[1] = CodTPV;
                MyArray[2] = FechaIni;
                MyArray[3] = HoraIni;
                MyArray[4] = SaldoIni;
                MyArray[5] = Turno;
                MyArray[6] = Apertura;
                MyArray[7] = Vendedor;
                MyArray[8] = SincrEmp;
                MyArray[9] = Activo;

                nas.ExecuteFunction = "PSTD_Apertura_Tienda_TPV";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Apertura_Tienda_TPV()", "Error al insertar Cliente: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }


        public DataSet Cierre_Tienda_TPV(string CodTienda, string CodTPV, string FechaIni,
          string HoraIni, string FechaFin, string HoraFin, string Activo, string Registrado,
          string DifCaja, string Recuento, string SaldoFin, string TotTrasp, string TotGastos,
          string TotCambio, string Turno)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Cierre_Tienda_TPV()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[15];
                MyArray[0] = CodTienda;
                MyArray[1] = CodTPV;
                MyArray[2] = FechaIni;
                MyArray[3] = HoraIni;
                MyArray[4] = FechaFin;
                MyArray[5] = HoraFin;
                MyArray[6] = Activo;
                MyArray[7] = Registrado;
                MyArray[8] = DifCaja;
                MyArray[9] = Recuento;
                MyArray[10] = SaldoFin;
                MyArray[11] = TotTrasp;
                MyArray[12] = TotGastos;
                MyArray[13] = TotCambio;
                MyArray[14] = Turno;

                nas.ExecuteFunction = "PSTD_Cierre_Tienda_TPV";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Cierre_Tienda_TPV()", "Error al insertar Cliente: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }

        public DataSet FinDiaArqueo(string CodTienda, string CodTPV, string FechaIni,
            string HoraIni, string FechaFin, string HoraFin, string Activo, string Registrado,
            string TotGastos, string TotCambio, string Turno, string Vendedor, string Arqueo,
            DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "FinDiaArqueo()", "ERROR: No se ha validado, debe abrir login");


            //REGISTRAMOS LA CABECERA DEL ARQUEO
            try
            {
                NavisionDB.NavisionDBNas nasCabArqueo = new NavisionDB.NavisionDBNas();
                nasCabArqueo.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[7];
                MyArray[0] = Arqueo;
                MyArray[1] = CodTienda;
                MyArray[2] = CodTPV;
                MyArray[3] = FechaIni;
                MyArray[4] = Turno;
                MyArray[5] = HoraFin;
                MyArray[6] = Vendedor;

                nasCabArqueo.ExecuteFunction = "RegistrarCabArqueo";
                nasCabArqueo.Parameters = MyArray;

                nasCabArqueo.SendParamsAsync(nasCabArqueo, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "FinDiaArqueo()", "Error al insertar cierre dia tpv: " + ex.Message);
                return DsResult;
            }

            //INTRODUCIMOS LAS LINEAS DEL ARQUEO
            try
            {
                int i;
                if (DatosLineas.Tables.Count > 0)
                {
                    for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                        NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasLineas.ExecuteFunction = "RegistrarLinArqueo";
                        string[] MyArray1 = new string[6];
                        MyArray1[0] = DatosLineas.Tables[0].Rows[i]["CodArqueo"].ToString();
                        MyArray1[1] = DatosLineas.Tables[0].Rows[i]["Numlin"].ToString();
                        MyArray1[2] = DatosLineas.Tables[0].Rows[i]["FormaPago"].ToString();
                        MyArray1[3] = DatosLineas.Tables[0].Rows[i]["ImporteReal"].ToString();
                        MyArray1[4] = DatosLineas.Tables[0].Rows[i]["ImporteArq"].ToString();
                        MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Diferencia"].ToString();
                        NasLineas.Parameters = MyArray1;
                        NasLineas.SendParamsAsync(NasLineas, "", false);

                    }
                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Fin_DiaArqueo()", "Error línea arqueo tpv: " + ex.Message);
                return DsResult;
            }

            //REALIZAMOS EL CIERRE DE DIA Y REGISTRAMOS EL DIARIO
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[13];
                MyArray[0] = Arqueo;
                MyArray[1] = CodTPV;
                MyArray[2] = FechaIni;
                MyArray[3] = HoraIni;
                MyArray[4] = FechaFin;
                MyArray[5] = HoraFin;
                MyArray[6] = Activo;
                MyArray[7] = Registrado;
                MyArray[8] = TotGastos;
                MyArray[9] = TotCambio;
                MyArray[10] = Turno;
                MyArray[11] = Vendedor;
                MyArray[12] = CodTienda;

                nas.ExecuteFunction = "PSTD_Cierre_TPV_Arqueo";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "FinDiaArqueo()", "Error al insertar cierre dia tpv: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }


        public DataSet FinDiaCaja(string CodTienda, string CodTPV, string FechaIni,
                      string HoraIni, string FechaFin, string HoraFin, string Activo, string Registrado,
                      string TotGastos, string TotCambio, string Turno, string Vendedor, string NumDoc,
                      DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "FinDiaCaja()", "ERROR: No se ha validado, debe abrir login");


            //INTRODUCIMOS LAS LINEAS DEL DIARIO
            try
            {
                int i;
                if (DatosLineas.Tables.Count > 0)
                {
                    for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                        NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasLineas.ExecuteFunction = "PSTD_RegDiario";
                        string[] MyArray1 = new string[15];
                        MyArray1[0] = DatosLineas.Tables[0].Rows[i]["FechaReg"].ToString();
                        MyArray1[1] = DatosLineas.Tables[0].Rows[i]["TipoDoc"].ToString();
                        MyArray1[2] = DatosLineas.Tables[0].Rows[i]["NumDoc"].ToString();
                        MyArray1[3] = DatosLineas.Tables[0].Rows[i]["TipoMov"].ToString();
                        MyArray1[4] = DatosLineas.Tables[0].Rows[i]["NumCta"].ToString();
                        MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Descripcion"].ToString();
                        MyArray1[6] = DatosLineas.Tables[0].Rows[i]["Importe"].ToString();
                        MyArray1[7] = DatosLineas.Tables[0].Rows[i]["TipoContra"].ToString();
                        MyArray1[8] = DatosLineas.Tables[0].Rows[i]["CtaContra"].ToString();
                        MyArray1[9] = DatosLineas.Tables[0].Rows[i]["CodTienda"].ToString();
                        MyArray1[10] = DatosLineas.Tables[0].Rows[i]["CodTPV"].ToString();
                        MyArray1[11] = DatosLineas.Tables[0].Rows[i]["NumTurno"].ToString();
                        MyArray1[12] = DatosLineas.Tables[0].Rows[i]["TipoMovTraspaso"].ToString();
                        MyArray1[13] = DatosLineas.Tables[0].Rows[i]["Signo"].ToString();
                        MyArray1[14] = Vendedor;

                        NasLineas.Parameters = MyArray1;
                        NasLineas.SendParamsAsync(NasLineas, "", false);

                    }
                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Fin_DiaCaja()", "Error línea diario fin tpv: " + ex.Message);
                return DsResult;
            }

            //REALIZAMOS EL CIERRE DE DIA Y REGISTRAMOS EL DIARIO
            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[13];
                MyArray[0] = NumDoc;
                MyArray[1] = CodTPV;
                MyArray[2] = FechaIni;
                MyArray[3] = HoraIni;
                MyArray[4] = FechaFin;
                MyArray[5] = HoraFin;
                MyArray[6] = Activo;
                MyArray[7] = Registrado;
                MyArray[8] = TotGastos;
                MyArray[9] = TotCambio;
                MyArray[10] = Turno;
                MyArray[11] = Vendedor;
                MyArray[12] = CodTienda;

                nas.ExecuteFunction = "PSTD_Cierre_TPV_Caja";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "FinDiaCaja()", "Error al insertar cierre dia tpv: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }

        //DPA++
        public DataSet FinDiaCajaFull(string CodTienda, string CodTPV, string FechaIni, string HoraIni, string FechaFin, string HoraFin, string Activo, string Registrado, string TotGastos, string TotCambio, string Turno, string Vendedor, string NumDoc, string SaldoInicial, string TotalVentas, string TotalCobros, string TotalPagos, string TotalDevolucionesCaja, string TotalRedondeos, string TotalVentasDia, string DiferenciaCaja, string FechaUltModificacion, string TotalRecuento, string TotalEntregasACta, string TotCobrosEfectivo, string TotPagosEfectivo, string TotVentasEfectivo, string TotalTarjetas, string TotalOtrasFormasDePago, string TotalTraspasoAFilial, string TotalVentasAbonoDto, string TotalEntregasCtaEfect, string TotalDevolucionesTarjeta, string TotalVales, string TotalValesRecibidos, string TotalEntregasCtaTarjeta, string Apertura, string SaldoFinalTPV, string VendedorCaja, string ReservasCanceladasEfectivo, string ReservasCanceladasCuenta, string CodArqueoDeCierre, DataSet DatosLineas)
        {
            System.Messaging.MessageQueueException exception;
            int num = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int num2 = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);
            string msqServerName = ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string msqsqltonavision = ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string msqsqlfromnavision = ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];
            DataSet set = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
            {
                return Utilidades.GenerarError("", "FinDiaCajaFull()", "ERROR: No se ha validado, debe abrir login");
            }
            try
            {
                if (DatosLineas.Tables.Count > 0)
                {
                    for (int i = 0; i <= (DatosLineas.Tables[0].Rows.Count - 1); i++)
                    {
                        NavisionDBNas nas = new NavisionDBNas();
                        nas.NasInitializeChannels(msqServerName, msqsqlfromnavision, msqsqltonavision);
                        nas.ExecuteFunction = "PSTD_RegDiario";
                        nas.Parameters = new string[] { DatosLineas.Tables[0].Rows[i]["FechaReg"].ToString(), DatosLineas.Tables[0].Rows[i]["TipoDoc"].ToString(), DatosLineas.Tables[0].Rows[i]["NumDoc"].ToString(), DatosLineas.Tables[0].Rows[i]["TipoMov"].ToString(), DatosLineas.Tables[0].Rows[i]["NumCta"].ToString(), DatosLineas.Tables[0].Rows[i]["Descripcion"].ToString(), DatosLineas.Tables[0].Rows[i]["Importe"].ToString(), DatosLineas.Tables[0].Rows[i]["TipoContra"].ToString(), DatosLineas.Tables[0].Rows[i]["CtaContra"].ToString(), DatosLineas.Tables[0].Rows[i]["CodTienda"].ToString(), DatosLineas.Tables[0].Rows[i]["CodTPV"].ToString(), DatosLineas.Tables[0].Rows[i]["NumTurno"].ToString(), DatosLineas.Tables[0].Rows[i]["TipoMovTraspaso"].ToString(), DatosLineas.Tables[0].Rows[i]["Signo"].ToString(), Vendedor };
                        nas.SendParamsAsync(nas, "", false);
                    }
                }
            }
            catch (System.Messaging.MessageQueueException exception1)
            {
                exception = exception1;
                return Utilidades.GenerarError(this.DBUser.UserCode, "FinDiaCajaFull()", "Error l\x00ednea diario fin tpv: " + exception.Message);
            }
            try
            {
                NavisionDBNas nas2 = new NavisionDBNas();
                nas2.NasInitializeChannels(msqServerName, msqsqlfromnavision, msqsqltonavision);
                string[] strArray2 = new string[] { NumDoc, CodTPV, FechaIni, HoraIni, FechaFin, HoraFin, Activo, Registrado, TotGastos, TotCambio, Turno, Vendedor, CodTienda };
                nas2.ExecuteFunction = "PSTD_Cierre_TPV_Caja";
                nas2.Parameters = strArray2;
                nas2.SendParamsAsync(nas2, "", false);
                nas2 = new NavisionDBNas();
                nas2.NasInitializeChannels(msqServerName, msqsqlfromnavision, msqsqltonavision);
                strArray2 = new string[] { 
            CodTPV, FechaIni, HoraIni, Turno, CodTienda, SaldoInicial, TotalVentas, TotalCobros, TotalPagos, TotalDevolucionesCaja, TotalRedondeos, TotalVentasDia, DiferenciaCaja, FechaUltModificacion, TotalRecuento, TotalEntregasACta, 
            TotCobrosEfectivo, TotPagosEfectivo, TotVentasEfectivo, TotalTarjetas, TotalOtrasFormasDePago, TotalTraspasoAFilial, TotalVentasAbonoDto, TotalEntregasCtaEfect, TotalDevolucionesTarjeta, TotalVales, TotalValesRecibidos, TotalEntregasCtaTarjeta, Apertura, SaldoFinalTPV, VendedorCaja, ReservasCanceladasEfectivo, 
            ReservasCanceladasCuenta, CodArqueoDeCierre
         };
                nas2.ExecuteFunction = "PSTD_Cierre_TPV_Caja_Completar";
                nas2.Parameters = strArray2;
                nas2.SendParamsAsync(nas2, "", false);
            }
            catch (System.Messaging.MessageQueueException exception2)
            {
                exception = exception2;
                return Utilidades.GenerarError(this.DBUser.UserCode, "FinDiaCajaFull()", "Error al insertar cierre dia tpv: " + exception.Message);
            }
            return set;
        }
        //DPA-- 

        public DataSet AltaProducto(string numero, string Descripcion, string Alias, string Clase, string UMBase, string GrContExis, string DtoFact, string GrEstad, string GrComision, string CalPrecio, string ValExis, string CodProveedor,
                           string PreIVAIncl, string GrIVANeg, string GrContProd, string GrImpuesto, string GrIVAProd, string MetBaja, string SistRepo, string PreVenta, string Coste,
                           string PolReaprov, string PolFabr, string CodFabricante, string CodCategoria, string CodGrProd)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "AltaProducto()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[26];
                MyArray[0] = numero;
                MyArray[1] = Descripcion;
                MyArray[2] = Alias;
                MyArray[3] = Clase;
                MyArray[4] = UMBase;
                MyArray[5] = GrContExis;
                MyArray[6] = DtoFact;
                MyArray[7] = GrEstad;
                MyArray[8] = GrComision;
                MyArray[9] = CalPrecio;
                MyArray[10] = ValExis;
                MyArray[11] = CodProveedor;
                MyArray[12] = PreIVAIncl;
                MyArray[13] = GrIVANeg;
                MyArray[14] = GrContProd;
                MyArray[15] = GrImpuesto;
                MyArray[16] = GrIVAProd;
                MyArray[17] = MetBaja;
                MyArray[18] = SistRepo;
                MyArray[19] = PreVenta;
                MyArray[20] = Coste;
                MyArray[21] = PolReaprov;
                MyArray[22] = PolFabr;
                MyArray[23] = CodFabricante;
                MyArray[24] = CodCategoria;
                MyArray[25] = CodGrProd;

                nas.ExecuteFunction = "PSTDAltaProducto";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "AltaProducto()", "Error al insertar producto: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }

        public DataSet RegDiario(string Fecharegistro,
                                       string Tipodocumento,
                                       string NDocumento,
                                       string TipoMovimiento,
                                       string NCuenta,
                                       string VDescripcion,
                                       string VImporteTex,
                                       string TipoContrapartidaTex,
                                       string NContrapartida,
                                       string CodTienda,
                                       string CodTPV,
                                       string NTurnoTex,
                                       string TipoMovTraspasoTex,
                                       string Signo, string Vendedor)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "RegDiario()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[15];
                MyArray[0] = Fecharegistro;
                MyArray[1] = Tipodocumento;
                MyArray[2] = NDocumento;
                MyArray[3] = TipoMovimiento;
                MyArray[4] = NCuenta;
                MyArray[5] = VDescripcion;
                MyArray[6] = VImporteTex;
                MyArray[7] = TipoContrapartidaTex;
                MyArray[8] = NContrapartida;
                MyArray[9] = CodTienda;
                MyArray[10] = CodTPV;
                MyArray[11] = NTurnoTex;
                MyArray[12] = TipoMovTraspasoTex;
                MyArray[13] = Signo;
                MyArray[14] = Vendedor;

                nas.ExecuteFunction = "PSTD_RegDiario";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "RegDiario()", "Error al insertar Cliente: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }

        public DataSet Reserva_Cancelar(string Ticket, string CodTPV,
                                        string CodTienda, DataSet MultiForma)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Reserva_Cancelar()", "ERROR: No se ha validado, debe abrir login");

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DE LA FACTURA
            try
            {
                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma = new NavisionDB.NavisionDBNas();
                        NasMultiForma.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma.ExecuteFunction = "PSTDMultiForma";
                        string[] MyArray1 = new string[25];
                        MyArray1[0] = "Dev. reserva";
                        MyArray1[1] = Ticket;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][1].ToString(); // Cod.Forma Pago
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][2].ToString(); // Importe
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][3].ToString(); // Cambio
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][4].ToString(); // Nº Vale
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][5].ToString(); // Tipo de Pago
                        MyArray1[8] = CodTPV;
                        MyArray1[9] = CodTienda;
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][6].ToString(); // Cobrado
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][7].ToString(); // Nº Vale Reserva
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][8].ToString(); // Nº cheque regalo
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][9].ToString(); // Tipo de vale

                        //BYL: Se elimina PSTDMultiForma2 
                        MyArray1[14] = MultiForma.Tables[0].Rows[i][10].ToString();// Nº tarjeta dto.
                        MyArray1[15] = MultiForma.Tables[0].Rows[i][11].ToString();// Financiera
                        MyArray1[16] = MultiForma.Tables[0].Rows[i][12].ToString();// Nº autorización financiera
                        MyArray1[17] = MultiForma.Tables[0].Rows[i][13].ToString();// "Gastos financiación"
                        MyArray1[18] = MultiForma.Tables[0].Rows[i][14].ToString();// % Gastos financiación
                        MyArray1[19] = MultiForma.Tables[0].Rows[i][15].ToString();// Cliente que financia
                        MyArray1[20] = MultiForma.Tables[0].Rows[i][16].ToString();// Forma pago gastos financieros
                        MyArray1[21] = MultiForma.Tables[0].Rows[i][17].ToString();// Cta. contrapartida gts. finan.
                        MyArray1[22] = MultiForma.Tables[0].Rows[i][18].ToString();// Nº tarjeta de cobro
                        MyArray1[23] = MultiForma.Tables[0].Rows[i][19].ToString();// Fecha registro
                        MyArray1[24] = MultiForma.Tables[0].Rows[i][20].ToString();// Nº tarjeta cobro gastos financieros
                        //BYL: Se elimina PSTDMultiForma2 

                        NasMultiForma.Parameters = MyArray1;
                        NasMultiForma.SendParamsAsync(NasMultiForma, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Cancelar()", "Error al insertar multiformas: " + ex.Message);
                return DsResult;
            }


            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DEL DOCUMENTO
            //CON EL RESTO DE INFORMACION QUE FALTA
            /* ### BYL: Se elimina PSTDMultiForma2  ###
            try
            {
                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma2 = new NavisionDB.NavisionDBNas();
                        NasMultiForma2.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma2.ExecuteFunction = "PSTDMultiForma2";
                        string[] MyArray1 = new string[14];
                        MyArray1[0] = "Dev. reserva";
                        MyArray1[1] = Ticket;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][10].ToString();// Nº tarjeta dto.
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][11].ToString();// Financiera
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][12].ToString();// Nº autorización financiera
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][13].ToString();// "Gastos financiación"
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][14].ToString();// % Gastos financiación
                        MyArray1[8] = MultiForma.Tables[0].Rows[i][15].ToString();// Cliente que financia
                        MyArray1[9] = MultiForma.Tables[0].Rows[i][16].ToString();// Forma pago gastos financieros
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][17].ToString();// Cta. contrapartida gts. finan.
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][18].ToString();// Nº tarjeta de cobro
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][19].ToString();// Fecha registro
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][20].ToString();// Nº tarjeta cobro gastos financieros

                        NasMultiForma2.Parameters = MyArray1;
                        NasMultiForma2.SendParamsAsync(NasMultiForma2, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reservas_Cancelar()", "Error al insertar multiformas2: " + ex.Message);
                return DsResult;
            }
            */

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[1];
                MyArray[0] = Ticket;

                nas.ExecuteFunction = "PSTD_ReservaCancelar";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Reserva_Cancelar()", "Error al insertar Cliente: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }


        public DataSet CabRep_Generar(string NumeroTicket, string AlmOrigen, string AlmDestino,
                        string FechaEnvio, string FechaReg, string FechaDoc, string AlmTransito,
                        string Vendedor, string NumBultos, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "CabRep_Generar()", "ERROR: No se ha validado, debe abrir login");

            //TODO: Comprobar la utilidad de los siguientes objetos !?
            NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter Da = new NavisionDBAdapter();
            NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            NavisionDBDataReader Rd = new NavisionDBDataReader();
            string NumeroSerieNuevo = string.Empty;

            NumeroSerieNuevo = NumeroTicket;

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[10];
                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = AlmOrigen;
                MyArray[2] = AlmDestino;
                MyArray[3] = FechaEnvio;
                MyArray[4] = FechaReg;
                MyArray[5] = FechaDoc;
                MyArray[6] = AlmTransito;
                MyArray[7] = Vendedor;
                MyArray[8] = NumBultos;                
                MyArray[9] = DatosLineas.Tables[0].Rows.Count.ToString();                             

                nas.ExecuteFunction = "PSTD_PedTransferencia_Generar";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "CabRep_Generar()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_GrabarLineaTransferencia";
                    string[] MyArray1 = new string[7];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i][0].ToString();//NumDoc
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i][1].ToString();//Cantidad
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i][2].ToString();//AlmOrigen
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i][3].ToString();//AlmDestino
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i][4].ToString();//NumLinea
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i][5].ToString();//CodVariante
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "CabRep_Generar()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            try
            {
                NavisionDB.NavisionDBNas nasReg = new NavisionDB.NavisionDBNas();
                nasReg.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[1];
                MyArray[0] = NumeroSerieNuevo;

                nasReg.ExecuteFunction = "PSTD_RegEnvio_Transferencia";
                nasReg.Parameters = MyArray;

                nasReg.SendParamsAsync(nasReg, "", false);


            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Factura");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Ventas_Crear()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }


            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet CabCarga_Generar(string NumCarga, string FechaEnvio, string NatuTrans,
                                        string ModTrans, string Puerto, string EspTrans,
                                        string CodTrans, string CondEnvio, string CodTPV,
                                        string CodTienda, string Estado, string FechaCreacion,
                                        string ServTrans, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "CabCarga_Generar()", "ERROR: No se ha validado, debe abrir login");

            NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter Da = new NavisionDBAdapter();
            NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            NavisionDBDataReader Rd = new NavisionDBDataReader();
            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumCarga;


            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[13];
                MyArray[0] = NumCarga;
                MyArray[1] = FechaEnvio;
                MyArray[2] = NatuTrans;
                MyArray[3] = ModTrans;
                MyArray[4] = Puerto;
                MyArray[5] = EspTrans;
                MyArray[6] = CodTrans;
                MyArray[7] = CondEnvio;
                MyArray[8] = CodTPV;
                MyArray[9] = CodTienda;
                MyArray[10] = Estado;
                MyArray[11] = FechaCreacion;
                MyArray[12] = ServTrans;

                nas.ExecuteFunction = "PSTD_CabCarga_Generar";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "CabCarga_Generar()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_GrabarLineaCarga";
                    string[] MyArray1 = new string[4];
                    MyArray1[0] = NumCarga;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i][1].ToString();//NumLin
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i][2].ToString();//NumEntrega
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i][3].ToString();//NumLinEntrega
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "CabCarga_Generar()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet Competidores(string CodProv, string Nombre, string Direccion,
                                string Direccion2, string Poblacion, string Pais,
                                string CP, string Provincia, string Mail,
                                string Web, string Telefono, string Fax,
                                string FechaMod, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Competidores()", "ERROR: No se ha validado, debe abrir login");

            NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter Da = new NavisionDBAdapter();
            NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            NavisionDBDataReader Rd = new NavisionDBDataReader();
            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = CodProv;


            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[13];
                MyArray[0] = CodProv;
                MyArray[1] = Nombre;
                MyArray[2] = Direccion;
                MyArray[3] = Direccion2;
                MyArray[4] = Poblacion;
                MyArray[5] = Pais;
                MyArray[6] = CP;
                MyArray[7] = Provincia;
                MyArray[8] = Mail;
                MyArray[9] = Web;
                MyArray[10] = Telefono;
                MyArray[11] = Fax;
                MyArray[12] = FechaMod;

                nas.ExecuteFunction = "PSTD_Competidores";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Competidores()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_PrecioCompetidores";
                    string[] MyArray1 = new string[6];
                    MyArray1[0] = CodProv;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i][0].ToString();//producto
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i][1].ToString();//fecha inicio
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i][2].ToString();//unidad medida
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i][3].ToString();//coste
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i][4].ToString();//fecha fin
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "Competidores()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet AltaPack(string numero, string Descripcion, string UMBase, string GrContExis,
                               string PreIVAIncl, string GrIVANeg, string GrContProd, string GrIVAProd,
                               string PreVenta, string CodCategoria, string FechaIni, string FechaFin,
                               string ProdOpcionales, string TipoPack, string MotBloqueo,
                               string CodTienda, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "AltaPack()", "ERROR: No se ha validado, debe abrir login");

            NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter Da = new NavisionDBAdapter();
            NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            NavisionDBDataReader Rd = new NavisionDBDataReader();
            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = numero;


            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[16];
                MyArray[0] = numero;
                MyArray[1] = Descripcion;
                MyArray[2] = UMBase;
                MyArray[3] = GrContExis;
                MyArray[4] = PreIVAIncl;
                MyArray[5] = GrIVANeg;
                MyArray[6] = GrContProd;
                MyArray[7] = GrIVAProd;
                MyArray[8] = PreVenta;
                MyArray[9] = CodCategoria;
                MyArray[10] = FechaIni;
                MyArray[11] = FechaFin;
                MyArray[12] = ProdOpcionales;
                MyArray[13] = TipoPack;
                MyArray[14] = MotBloqueo;
                MyArray[15] = CodTienda;

                nas.ExecuteFunction = "DarAltaProductoPack";
                nas.Parameters = MyArray;
                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "AltaPack()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }
            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_ComponentesPack";
                    string[] MyArray1 = new string[11];
                    MyArray1[0] = DatosLineas.Tables[0].Rows[i][0].ToString();//CodPack
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i][1].ToString();//NumLinea
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i][2].ToString();//Tipo
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i][3].ToString();//CodProd
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i][4].ToString();//Descripcion
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i][5].ToString();//UDM
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i][6].ToString();//Cantidad
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i][7].ToString();//Coste
                    MyArray1[8] = DatosLineas.Tables[0].Rows[i][8].ToString();//CosteTotal
                    MyArray1[9] = DatosLineas.Tables[0].Rows[i][9].ToString();//Obligatorio
                    MyArray1[10] = DatosLineas.Tables[0].Rows[i][10].ToString();//TipoObli

                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "AltaPack()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet CabCarga_Liquidar(string NumCarga, string Usuario, DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "CabCarga_Liquidar()", "ERROR: No se ha validado, debe abrir login");

            NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter Da = new NavisionDBAdapter();
            NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            NavisionDBDataReader Rd = new NavisionDBDataReader();
            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumCarga;

            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_CargaLiquidar";
                    string[] MyArray1 = new string[7];
                    MyArray1[0] = NumCarga;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i][1].ToString();//NumLin
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i][2].ToString();//CdadEntregada
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i][3].ToString();//CdadDevuelta
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i][4].ToString();//ImpLiq
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i][5].ToString();//FormaPago
                    MyArray1[6] = Usuario;                                    //NumLinEntrega
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "CabCarga_Liquidar()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet DiarioProd_Generar(DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "DiarioProd_Generar()", "ERROR: No se ha validado, debe abrir login");

            //TODO: Comprobar utilidad de los siguientes objetos !?
            NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter Da = new NavisionDBAdapter();
            NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            NavisionDBDataReader Rd = new NavisionDBDataReader();
            string NumeroSerieNuevo = "";

            //NumeroSerieNuevo = NumeroTicket;

            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTD_DiarioProd_Generar";
                    string[] MyArray1 = new string[11];
                    MyArray1[0] = DatosLineas.Tables[0].Rows[i][0].ToString();//Libro
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i][1].ToString();//Seccion
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i][2].ToString();//Nº linea
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i][3].ToString();//Producto
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i][4].ToString();//Fecha
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i][5].ToString();//Tipo Mov
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i][6].ToString();//Nº documento
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i][7].ToString();//Almacen
                    MyArray1[8] = DatosLineas.Tables[0].Rows[i][8].ToString();//Cantidad
                    MyArray1[9] = DatosLineas.Tables[0].Rows[i][9].ToString();//Cantidad calculada
                    MyArray1[10] = DatosLineas.Tables[0].Rows[i][10].ToString();//Cantidad fisica
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "DiarioProd_Generar()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Diario Producto");
                this.BorrarCabecera(NumeroSerieNuevo, "Diario Producto");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("DiarioProd", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["DiarioProd"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet HistLinFra_Generar(DataSet DatosLineas)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "HistLinFra_Generar()", "ERROR: No se ha validado, debe abrir login");

            NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
            NavisionDBAdapter Da = new NavisionDBAdapter();
            NavisionDBCommand Cmd = new NavisionDBCommand(this.Connection);
            NavisionDBNumSerie NumSerie = new NavisionDBNumSerie(this.DBUser, this.Connection);
            NavisionDBDataReader Rd = new NavisionDBDataReader();
            string NumeroSerieNuevo = "";

            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTDHistLinFraGenerar";
                    string[] MyArray1 = new string[3];
                    MyArray1[0] = DatosLineas.Tables[0].Rows[i][0].ToString();//NumFactura
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i][1].ToString();//NumLinea
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i][2].ToString();//Numcheque
                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "HistLinFra_Generar()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "HistLinFra");
                this.BorrarCabecera(NumeroSerieNuevo, "HistLinFra");
                return DsResult;
            }

            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("HistLinFra", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["HistLinFra"] = NumeroSerieNuevo;
            return DsResult;
        }


        public DataSet Traer_Pago_Central(string NumDoc)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Traer_Pago_Central()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 21; // Tabla Vendedores 
                Dt.AddColumn(3);   //Nº cliente
                Dt.AddColumn(6);   //Nº documento
                Dt.AddColumn(5);   //Tipo documento 
                Dt.AddColumn(16);   //Importe pendiente (DL)


                Dt.KeyInNavisionFormat = "Document No.,Document Type,Customer No.";
                Dt.AddFilter(6, NumDoc); //Nº documento

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Traer_Pago_Central(): ", ex.Message);
            }
        }

        public DataSet Cheques_Central(string NumDoc)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Cheques_Central()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Dt.TableNo = 50011; // Cheques regalo 
                Dt.TableName = "Documentos regalo";
                Dt.AddFilter(1, NumDoc); //Nº documento

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Cheques_Central(): ", ex.Message);
            }
        }

        public DataSet Cheques_CentralArti(string NumDoc)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Cheques_CentralArti()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                //Dt.TableNo = 50078; // Cheques regalo 
                Dt.TableName = "Documentos regalo";
                Dt.AddFilter(1, NumDoc); //Nº documento

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Cheques_CentralArti(): ", ex.Message);
            }
        }

        public DataSet Traer_CosteProducto(string NumProducto)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Traer_CosteProducto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 27; // Producto
                Dt.AddColumn(1);    //No.
                Dt.AddColumn(22);   //Unit Cost


                Dt.AddFilter(1, NumProducto); //Nº documento

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Traer_CosteProducto(): ", ex.Message);
            }
        }

        public DataSet Traer_StockProducto(string NumProducto,
                                           string Usuario)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Traer_StockProducto()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 27; // Producto
                Dt.AddColumn(1);    //No.
                Dt.AddColumn(68);   //Inventory o Existencias


                Dt.AddFilter(1, NumProducto);   //Nº documento
                Dt.AddFilter(67, Usuario);      //Location Filter

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Traer_StockProducto(): ", ex.Message);
            }
        }



        public DataSet Abono_Pendiente(string NumAbono)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Abono_Pendiente()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                // Ponemos números para la Migración a versión 4.0 SP2

                dt.TableNo = 21; //Mov. cliente
                dt.AddColumn(3);
                dt.AddColumn(5);
                dt.AddColumn(6);
                dt.AddColumn(16);
                dt.AddColumn(36);

                dt.KeyInNavisionFormat = "Document No.,Document Type,Customer No.";
                dt.AddFilter(6, NumAbono);
                dt.AddFilter(5, "Credit Memo");
                dt.AddFilter(36, "true");
                //dt.AddFilter(3, CodCliente);
                da.AddTable(dt);


                dt.Reset();
                dt.TableNo = 114; //Histórico cab. abono venta

                //dt.AddColumn(2);    // Venta a-Nº cliente   
                dt.AddColumn(20);
                dt.AddColumn(28);
                dt.AddColumn(43);   // vendedor                                     
                dt.AddColumn(61);
                dt.AddColumn(70);   // CIF/NIF
                dt.AddColumn(79);
                dt.AddColumn(80);
                dt.AddColumn(50118);     // <Crear nota de entrega>
                //dt.AddColumn(50126);        //imp. anticipo
                dt.AddColumn(50111);
                dt.AddColumn(50112);

                da.AddTable(dt);
                da.AddRelation("Cust. Ledger Entry", "Document No.", "Sales Cr.Memo Header", "No.", NavisionDBAdapter.JoinType.Parent_Outer_Join, false);

                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Abono_Pendiente()", ex.Message);
            }
        }





        ///////////////////////////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////


        public DataSet DevEfectivo(string Ticket, string Inicio, string CodForma,
                               string Imp, string TipoPago, string CodTPV,
                               string CodTienda, string TipoVale, string FechaReg,
                               string NumCli, string NumTurno, string CodVendedor)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "DevEfectivo()", "ERROR: No se ha validado, debe abrir login");

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[12];
                MyArray[0] = Ticket;
                MyArray[1] = Inicio;
                MyArray[2] = CodForma;
                MyArray[3] = Imp;
                MyArray[4] = TipoPago;
                MyArray[5] = CodTPV;
                MyArray[6] = CodTienda;
                MyArray[7] = TipoVale;
                MyArray[8] = FechaReg;
                MyArray[9] = NumCli;
                MyArray[10] = NumTurno;
                MyArray[11] = CodVendedor;

                nas.ExecuteFunction = "PSTD_DevEfectivo";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {

                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "DevEfectivo()", "Error al DevEfectivo: " + ex.Message);
                return DsResult;
            }
            return DsResult;
        }

        public DataSet GrabarCabeceraTarjetaRegalo(string NumeroTicket, string Cliente, string CodTienda,
                                      string CodTPV, string EnvioNombre, string EnvioDireccion, string EnvioCP,
                                      string EnvioPoblacion, string EnvioProvincia, string EnvioAtencion,
                                      string CodFormaPago, string CodVendedor, string FechaTicket,
                                      string CrearEntrega, string GruDtoCol, string NumFidelizacion,
                                      string CobroTrans, string FactAuto, string RepInmediata, string DesColectivo,
                                      string AlmDestino, string EnvioObserv, string NumLinPago,
                                      string CodVenAnticipo, string FechaEnvio, string ActTRegalo,
                                      string DesactTRegalo, DataSet DatosLineas, DataSet MultiForma)
        {
            int TimeoutCabecera = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_CABECERA"]);
            int TimeoutLineas = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TIME_OUT_MILISEG_GRABAR_LINEAS"]);

            string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
            string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
            string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];

            DataSet DsResult = new DataSet();
            if ((this.DBUser == null) || (this.DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "GrabarCabeceraTarjetaRegalo()", "ERROR: No se ha validado, debe abrir login");

            string NumeroSerieNuevo = "";

            NumeroSerieNuevo = NumeroTicket;

            try
            {
                NavisionDB.NavisionDBNas nas = new NavisionDB.NavisionDBNas();
                nas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray = new string[18];
                MyArray[0] = NumeroSerieNuevo;
                MyArray[1] = Cliente;
                MyArray[2] = CodTienda;
                MyArray[3] = CodFormaPago;
                MyArray[4] = CodVendedor;
                MyArray[5] = FechaTicket;
                MyArray[6] = CrearEntrega;
                MyArray[7] = GruDtoCol;
                MyArray[8] = NumFidelizacion;
                MyArray[9] = CobroTrans;
                MyArray[10] = FactAuto;
                MyArray[11] = RepInmediata;
                MyArray[12] = DesColectivo;
                MyArray[13] = AlmDestino;
                MyArray[14] = ActTRegalo;
                MyArray[15] = DesactTRegalo;

                nas.ExecuteFunction = "PSTDActivarTarjetaRegalo";
                nas.Parameters = MyArray;

                nas.SendParamsAsync(nas, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "ActivarTarjetaRegalo()", "Error al insertar la cabecera: " + ex.Message + ". Cabecera borrada: " + NumeroSerieNuevo);
                return DsResult;
            }

            try
            {
                int i;
                for (i = 0; i <= DatosLineas.Tables[0].Rows.Count - 1; i++)
                {
                    NavisionDB.NavisionDBNas NasLineas = new NavisionDB.NavisionDBNas();
                    NasLineas.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                    NasLineas.ExecuteFunction = "PSTDGrabarLineaTarjetaRegalo";
                    string[] MyArray1 = new string[16];
                    MyArray1[0] = NumeroSerieNuevo;
                    MyArray1[1] = DatosLineas.Tables[0].Rows[i]["No."].ToString();
                    MyArray1[2] = DatosLineas.Tables[0].Rows[i]["Quantity"].ToString();
                    MyArray1[3] = DatosLineas.Tables[0].Rows[i]["UnitPrice"].ToString();
                    MyArray1[4] = DatosLineas.Tables[0].Rows[i]["LineDiscount"].ToString();
                    MyArray1[5] = DatosLineas.Tables[0].Rows[i]["Description"].ToString();
                    MyArray1[6] = DatosLineas.Tables[0].Rows[i]["Type"].ToString();
                    MyArray1[7] = DatosLineas.Tables[0].Rows[i]["CrearEntrega"].ToString();
                    MyArray1[8] = DatosLineas.Tables[0].Rows[i]["CdadEntregar"].ToString();
                    MyArray1[9] = DatosLineas.Tables[0].Rows[i]["SecPack"].ToString();
                    MyArray1[10] = DatosLineas.Tables[0].Rows[i]["DtoCol"].ToString();
                    MyArray1[11] = DatosLineas.Tables[0].Rows[i]["ImpDtoCol"].ToString();
                    MyArray1[12] = DatosLineas.Tables[0].Rows[i]["NumLinea"].ToString();
                    MyArray1[13] = DatosLineas.Tables[0].Rows[i]["Cancelada"].ToString();
                    MyArray1[14] = DatosLineas.Tables[0].Rows[i]["Variante"].ToString();
                    MyArray1[15] = DatosLineas.Tables[0].Rows[i]["NumTarjetaRegalo"].ToString();

                    NasLineas.Parameters = MyArray1;
                    NasLineas.SendParamsAsync(NasLineas, "", false);

                }
            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "PSTDGrabarLineaTarjetaRegalo()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            //LLAMAMOS A LA FUNCION DE INTRODUCIR LAS MULTIFORMAS DE PAGO DE LA FACTURA
            try
            {
                int i;
                if (MultiForma.Tables.Count > 0)
                {
                    for (i = 0; i <= MultiForma.Tables[0].Rows.Count - 1; i++)
                    {
                        NavisionDB.NavisionDBNas NasMultiForma = new NavisionDB.NavisionDBNas();
                        NasMultiForma.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                        NasMultiForma.ExecuteFunction = "PSTDMultiFormaTRegalo";
                        string[] MyArray1 = new string[15];
                        MyArray1[0] = "Reserva";
                        MyArray1[1] = NumeroSerieNuevo;
                        MyArray1[2] = MultiForma.Tables[0].Rows[i][0].ToString(); // Nº Linea
                        MyArray1[3] = MultiForma.Tables[0].Rows[i][1].ToString(); // Cod.Forma Pago
                        MyArray1[4] = MultiForma.Tables[0].Rows[i][2].ToString(); // Importe
                        MyArray1[5] = MultiForma.Tables[0].Rows[i][3].ToString(); // Cambio
                        MyArray1[6] = MultiForma.Tables[0].Rows[i][4].ToString(); // Nº Vale
                        MyArray1[7] = MultiForma.Tables[0].Rows[i][5].ToString(); // Tipo de Pago
                        MyArray1[8] = CodTPV;
                        MyArray1[9] = CodTienda;
                        MyArray1[10] = MultiForma.Tables[0].Rows[i][6].ToString(); // Cobrado
                        MyArray1[11] = MultiForma.Tables[0].Rows[i][7].ToString(); // Nº Vale Reserva
                        //MyArray1[12] = MultiForma.Tables[0].Rows[i][8].ToString(); // Nº cheque regalo
                        MyArray1[12] = MultiForma.Tables[0].Rows[i][8].ToString(); // Tipo de vale
                        MyArray1[13] = MultiForma.Tables[0].Rows[i][9].ToString(); // Nº tarjeta regalo
                        MyArray1[14] = MultiForma.Tables[0].Rows[i][19].ToString(); //Fecha de registro
                        NasMultiForma.Parameters = MyArray1;
                        NasMultiForma.SendParamsAsync(NasMultiForma, "", false);
                    }
                }

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "PSTDMultiFormaTRegalo()", "Error al insertar multiformas: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            //REGISTRAMOS EL DOCUMENTO
            try
            {
                NavisionDB.NavisionDBNas nasLanzar = new NavisionDB.NavisionDBNas();
                nasLanzar.NasInitializeChannels(nombre_servidor_colas, colaFromNav, colaToNav);
                string[] MyArray2 = new string[1];
                MyArray2[0] = NumeroSerieNuevo;

                nasLanzar.ExecuteFunction = "PSTDRegistrarDocTarjetaRegalo";
                nasLanzar.Parameters = MyArray2;

                nasLanzar.SendParamsAsync(nasLanzar, "", false);

            }
            catch (System.Messaging.MessageQueueException ex)
            {
                DsResult = Utilidades.GenerarError(this.DBUser.UserCode, "ActivarTarjetaRegalo()", "Error al insertar línea: " + ex.Message);
                this.BorrarLineas(NumeroSerieNuevo, "Pedido");
                this.BorrarCabecera(NumeroSerieNuevo, "Pedido");
                return DsResult;
            }

            //Generamos resultado OK
            DsResult = Utilidades.GenerarResultado("Correcto");
            DsResult.Tables[0].Columns.Add("NumPedido", Type.GetType("System.String"));
            DsResult.Tables[0].Rows[0]["NumPedido"] = NumeroSerieNuevo;
            return DsResult;
        }

        public DataSet Sincronizar_CodificacionErroresTR()
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "Sincronizar_CodificacionErroresTR()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable Dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                Dt.TableNo = 50078;       //"Codificación errores TR"
                Dt.AddColumn(1);          //"Código error"
                Dt.AddColumn(2);          //"Texto error"

                da.AddTable(Dt);
                da.Fill(ref ds, false);

                Utilidades.CompletarDataSet(ref ds, false, false);
                //Primer parametro false = string vacia o true = espacio en blanco
                //Segundo parametro fechas true = no mete nada y false 01010001

                return ds;
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "Sincronizar_CodificacionErroresTR(): ", ex.Message);
            }
        }


        public DataSet TPVQueueResponse(string Id, bool Result, string Fecha, string Hora, string Msg)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPVResponse()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();


                dt.TableName = "TPV Response";
                da.AddTable(dt);
                ds = dt.GenerateStructure();

                DataRow dr = ds.Tables[0].NewRow();
                dr["Entry No."] = 0;
                dr["ID"] = Id;
                dr["Shop Code"] = DBUser.UserCode;

                if (!String.IsNullOrEmpty(Fecha))
                { 
                    DateTime FechaDT = DateTime.Parse(Fecha);                    
                    dr["Execution Date"] = FechaDT.ToString("dd/MM/yyyy");
                }
                else
                {
                    dr["Execution Date"] = DateTime.Now.ToString("dd/MM/yyyy");
                }

                if (!String.IsNullOrEmpty(Hora))
                {
                    DateTime HoraDT = DateTime.Parse(Hora);
                    dr["Execution Time"] = HoraDT.ToString("HH:mm:ss");
                }
                else
                {
                    dr["Execution Time"] = DateTime.Now.ToString("HH:mm:ss");
                }

                int len = Msg.Length;

                if (len < 250)
                {
                    dr["Message 1"] = Msg.Substring(0, len);
                }
                else
                {
                    dr["Message 1"] = Msg.Substring(0, 250);
                }

                if (Msg.Length > 250)
                {
                    len -= 250;
                    if (len < 250)
                    {
                        dr["Message 2"] = Msg.Substring(250, len);
                    }
                    else
                    {
                        dr["Message 2"] = Msg.Substring(250, 250);
                    }

                }

                if (Msg.Length > 500)
                {
                    len -= 250;
                    if (len < 250)
                    {
                        dr["Message 3"] = Msg.Substring(500, len);
                    }
                    else
                    {
                        dr["Message 3"] = Msg.Substring(500, 250);
                    }
                }

                if (Msg.Length > 750)
                {
                    len -= 250;
                    if (len < 250)
                    {
                        dr["Message 4"] = Msg.Substring(750, len);
                    }
                    else
                    {
                        dr["Message 4"] = Msg.Substring(750, 250);
                    }
                }


                if (Result)
                {
                    dr["Status"] = "Success";
                }
                else
                {
                    dr["Status"] = "Error";
                }

                ds.Tables[0].Rows.Add(dr);

                da.InsertItem = ds;

                try
                {
                    this.Connection.BWT();
                    da.Update();
                    this.Connection.EWT();
                }
                catch (Exception ex)
                {
                    this.Connection.AWT();
                    return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "TPVResponse(): ", ex.Message);
                }

                return Utilidades.GenerarResultado("Ok");
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "TPVResponse(): ", ex.Message);
            }
        }        

        public DataSet TPVQueueRequest(string Fecha)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPVQueueRequest()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableName = "TPV Request";
                dt.AddColumn("ID");                
                dt.AddColumn("Expiration Date/Time");
                dt.AddColumn("Earliest Start Date/Time");
                dt.AddColumn("Object Type to Run");
                dt.AddColumn("Object ID to Run");                                
                dt.AddColumn("Maximum No. of Attempts to Run");                
                dt.AddColumn("Status");
                dt.AddColumn("Priority");                
                dt.AddColumn("Parameter String");
                dt.AddColumn("Recurring Job");
                dt.AddColumn("No. of Minutes between Runs");
                dt.AddColumn("Run on Mondays");
                dt.AddColumn("Run on Tuesdays");
                dt.AddColumn("Run on Wednesdays");
                dt.AddColumn("Run on Thursdays");
                dt.AddColumn("Run on Fridays");
                dt.AddColumn("Run on Saturdays");
                dt.AddColumn("Run on Sundays");
                dt.AddColumn("Starting Time");
                dt.AddColumn("Ending Time");
                dt.AddColumn("Synch Results");

                dt.KeyInNavisionFormat = "Shop Code,Creation Date,Active";

                dt.AddFilter("Shop Code", DBUser.UserCode + "|''");

                if (!String.IsNullOrEmpty(Fecha))
                {
                    dt.AddFilter("Creation Date", DateTime.Parse(Fecha).ToString("dd/MM/yyyy") + "..");
                }
                
                dt.AddFilter("Active", "true");

                da.AddTable(dt);
                da.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);

                //string guid = ds.Tables[0].Rows[0].ItemArray.GetValue(0).ToString();                

                da = new NavisionDBAdapter();                
                dt.Reset();
                DataSet dsLog = new DataSet();

                dt.TableName = "TPV Response";
                da.AddTable(dt);
                dsLog = dt.GenerateStructure();

                DataRow dr;
                int lItems = ds.Tables[0].Rows.Count;
                for (int i = 0; i < lItems; i++)
                {
                    dr = dsLog.Tables[0].NewRow();
                    dr["Entry No."] = 0;
                    dr["ID"] = "{" + ds.Tables[0].Rows[i].ItemArray.GetValue(0).ToString() + "}";
                    dr["Shop Code"] = DBUser.UserCode;
                    dr["Status"] = "Downloaded";
                    dr["Execution Date"] = DateTime.Now.ToString("dd/MM/yyyy");
                    dr["Execution Time"] = DateTime.Now.ToString("HH:mm:ss");
                    dsLog.Tables[0].Rows.Add(dr);
                }                

                da.InsertItem = dsLog;

                try
                {
                    this.Connection.BWT();
                    da.Update();                 
                    this.Connection.EWT();
                }
                catch (Exception excep)
                {
                    this.Connection.AWT();
                    throw new Exception("TPVQueueRequest(): " + excep.Message);
                }

                return ds;

            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError(this.DBUser.UserCode, "TPVQueueRequest(): ", ex.Message);
            }
        }

        public DataSet Update_Customer(string CustCode)
        {
            if ((DBUser == null) || (DBUser.UserCode == ""))
                return Utilidades.GenerarError("", "Update_Customer()", "ERROR: No se ha validado, debe abrir login");
            try
            {
                NavisionDBTable dt = new NavisionDBTable(this.Connection, this.DBUser);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableName = "Customer";
                dt.AddColumn("No.");                
                dt.AddColumn("Name");
                dt.AddColumn("Name 2");
                dt.AddColumn("Address");
                dt.AddColumn("City");
                dt.AddColumn("Post Code");                
                dt.AddColumn("County");
                dt.AddColumn("Phone No.");
                dt.AddColumn("E-Mail");
                dt.AddColumn("VAT Registration No.");
                dt.AddColumn("Payment Method Code");
                dt.AddColumn("Payment Terms Code");
                dt.AddColumn("Customer Disc. Group");
                dt.AddColumn("Enviar Newsletter");
                
                dt.AddFilter(1, CustCode);

                da.AddTable(dt);
                da.Fill(ref ds, false);
                Utilidades.CompletarDataSet(ref ds, false, false);
                
                return ds;
            }
            catch (Exception ex)
            {
                return Utilidades.GenerarError(this.DBUser.UserCode, "Update_Customer()", ex.Message);
            }
        }

    }
}
