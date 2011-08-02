using System;
using System.Data;
using System.Web.Services;

/// <summary>
/// Descripción breve de ServicioTPV
/// </summary>
[WebService(Namespace = "http://tempuri.org/")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
public class ServicioTPV : System.Web.Services.WebService
{
    private MiddleWareTPVCentral.TPV TPV;

    public ServicioTPV()
    {
        //Eliminar la marca de comentario de la línea siguiente si utiliza los componentes diseñados 
        //InitializeComponent(); 
    }

    [WebMethod(true)]
    public DataSet ST_Login(string UserId, string Password)
    {        
        DataSet DsRes = new DataSet();
        if ((String.IsNullOrEmpty(UserId)) || (String.IsNullOrEmpty(Password)))
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "", "Debe introducir usuario y contraseña.");

        Session["DBUser"] = MiddleWareTPVCentral.Utilidades.Abrir_Login(UserId, Password, ref DsRes, WebServiceTPVCentral.Global.navConection);
        return DsRes;
    }

    [WebMethod(true)]
    public DataSet TPV_Abono_Generar(string Usuario, string password,
                                       string NumeroTicket,
                                       string Cliente,
                                       string CodTienda,
                                       string CodDirEnvio,
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
                                       string IMporteDevolucion,
                                       string ImporteEntregado,
                                       string NumTicketAbonado,
                                       string CrearEntrega,
                                       string EnvioObserv,
                                       string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";

            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);



            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Abonos_Crear(NumeroTicket, Cliente, CodTienda, CodDirEnvio, DatosLineas,
                EnvioNombre, EnvioDireccion, EnvioCP, EnvioPoblacion, EnvioProvincia, EnvioAtencion,
                CodFormaPago, CodVendedor, FechaTicket, CodTPV, HoraTicket,
                IMporteDevolucion, ImporteEntregado, NumTicketAbonado, CrearEntrega, EnvioObserv);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Abono_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Abono_Generar2(string Usuario,
                                      string password,
                                      string NumeroTicket,
                                      string Cliente,
                                      string CodTienda,
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
                                      string TotalApagar,
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
                                      string DatosLineasStr,
                                      string DatosMultiStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich1 = Cx_Log1 + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(DatosMultiStr);
            fich1.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(nomFich1);


            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);

            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));

            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(new System.IO.StringReader(DatosMultiStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Abono_Crear2(NumeroTicket, Cliente, CodTienda, DatosLineas, EnvioNombre,
                EnvioDireccion, EnvioCP, EnvioPoblacion, EnvioProvincia, CodFormaPago, CodVendedor,
                FechaTicket, CodTPV, HoraTicket, TotalApagar, ImporteVenta, AplicarDto, NumTarjetaDto,
                GruDtoCol, NumFidelizacion, CrearEntrega, CobroTrans, FactAuto, RepInmediata,
                NumFacAbonar, EnvioObserv, DesColectivo, FechaEnvio, DatosMulti);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario + "-" + NumeroTicket, "TPV_Abono_Generar2()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Actualizar_Lineas_Reposicion(string Usuario,
                                                    string password,
                                                    string Documento,
                                                    string Linea,
                                                    string CantidadRecibida)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Actualizar_lineas_Reposicion(Documento, Linea, CantidadRecibida);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Actualizar_Lineas_Reposicion()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_AltaCliente(string Usuario,
                                   string password,
                                   string Numero,
                                   string Nombre,
                                   string Direccion,
                                   string Cp,
                                   string Poblacion,
                                   string Provincia,
                                   string Telefono,
                                   string Telefono2,
                                   string Email,
                                   string NIF,
                                   string CodFormaPago,
                                   string CodVendedor,
                                   string CodTerminosPago,
                                   string GrupoContCliente,
                                   string GrupoContNegocio,
                                   string GrupoRegIVANeg,
                                   string Newsletter,
                                   string NombreDos,
                                   string ShopCode)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.AltaCliente(Numero, Nombre, Direccion, Cp, Poblacion, Provincia, Telefono, Telefono2,
                                   Email, NIF, CodFormaPago, CodVendedor, CodTerminosPago, GrupoContCliente,
                                   GrupoContNegocio, GrupoRegIVANeg, Newsletter, NombreDos, ShopCode);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Ficha_NIF()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_AltaPack(string Usuario,
                                string password,
                                string numero,
                                string Descripcion,
                                string UMBase,
                                string GrContExis,
                                string PreIVAIncl,
                                string GrIVANeg,
                                string GrContProd,
                                string GrIVAProd,
                                string PreVenta,
                                string CodCategoria,
                                string FechaIni,
                                string FechaFin,
                                string ProdOpcionales,
                                string TipoPack,
                                string MotBloqueo,
                                string CodTienda,
                                string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            MiddleWareTPVCentral.Utilidades.GenerarError("pasa por aquí", "TPV_AltaPck()", "");
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.AltaPack(numero, Descripcion, UMBase, GrContExis, PreIVAIncl,
                                GrIVANeg, GrContProd, GrIVAProd, PreVenta, CodCategoria,
                                FechaIni, FechaFin, ProdOpcionales, TipoPack, MotBloqueo, CodTienda,
                                DatosLineas);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_AltaPack()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_AltaProducto(string Usuario,
                                    string password,
                                    string numero,
                                    string Descripcion,
                                    string Alias,
                                    string Clase,
                                    string UMBase,
                                    string GrContExis,
                                    string DtoFact,
                                    string GrEstad,
                                    string GrComision,
                                    string CalPrecio,
                                    string ValExis,
                                    string CodProveedor,
                                    string PreIVAIncl,
                                    string GrIVANeg,
                                    string GrContProd,
                                    string GrImpuesto,
                                    string GrIVAProd,
                                    string MetBaja,
                                    string SistRepo,
                                    string PreVenta,
                                    string Coste,
                                    string PolReaprov,
                                    string PolFabr,
                                    string CodFabricante,
                                    string CodCategoria,
                                    string CodGrProd)
    {
        try
        {
            MiddleWareTPVCentral.Utilidades.GenerarError("Ha pasado", "TPV_AltaProducto()", "");
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }
            MiddleWareTPVCentral.Utilidades.GenerarError("Paso1", "TPV_AltaProducto()", "");
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.AltaProducto(numero, Descripcion, Alias, Clase, UMBase, GrContExis, DtoFact,
                       GrEstad, GrComision, CalPrecio, ValExis, CodProveedor, PreIVAIncl, GrIVANeg,
                       GrContProd, GrImpuesto, GrIVAProd, MetBaja, SistRepo, PreVenta, Coste,
                       PolReaprov, PolFabr, CodFabricante, CodCategoria, CodGrProd);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_AltaProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_AltaTarifa(string Usuario,
                                  string password,
                                  string CodProducto,
                                  string TipoVenta,
                                  string CodVenta,
                                  string FechaIni,
                                  string Divisa,
                                  string Variante,
                                  string UDS,
                                  string CdadMin,
                                  string PreVenta,
                                  string IvaInc,
                                  string DtoFactura,
                                  string IvaNegocio,
                                  string FechaFin,
                                  string DtoLinea,
                                  string FechaMod)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.AltaTarifa(CodProducto, TipoVenta, CodVenta, FechaIni, Divisa,
                                  Variante, UDS, CdadMin, PreVenta, IvaInc, DtoFactura,
                                  IvaNegocio, FechaFin, DtoLinea, FechaMod);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_AltaTarifa()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Apertura_Tienda_TPV(string Usuario,
                                           string password,
                                           string CodTienda,
                                           string CodTPV,
                                           string FechaIni,
                                           string HoraIni,
                                           string SaldoIni,
                                           string Turno,
                                           string Apertura,
                                           string Vendedor,
                                           string SincrEmp,
                                           string Activo)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Apertura_Tienda_TPV(CodTienda, CodTPV, FechaIni, HoraIni, SaldoIni,
                                           Turno, Apertura, Vendedor, SincrEmp, Activo);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Apertura_Tienda_TPV()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Buscar_Clientes(string Usuario,
                                       string password,
                                       string Nombre,
                                       string Apellidos,
                                       string NIF,
                                       string Telefono)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Buscar_Clientes(Nombre, Apellidos, NIF, Telefono);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Buscar_Clientes()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Buscar_Clientes_Contador(string Usuario,
                                                string password,
                                                string Nombre,
                                                string Apellidos,
                                                string NIF,
                                                string Telefono)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Buscar_Clientes_Contador(Nombre, Apellidos, NIF, Telefono);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Buscar_Clientes_Contador()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_CabCarga_Generar(string Usuario,
                                        string password,
                                        string NumCarga,
                                        string FechaEnvio,
                                        string NatuTrans,
                                        string ModTrans,
                                        string Puerto,
                                        string EspTrans,
                                        string CodTrans,
                                        string CondEnvio,
                                        string CodTPV,
                                        string CodTienda,
                                        string Estado,
                                        string FechaCreacion,
                                        string ServTrans,
                                        string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            MiddleWareTPVCentral.Utilidades.GenerarError("pasa por aquí", "TPV_CabCarga_Generar()", "");
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.CabCarga_Generar(NumCarga, FechaEnvio, NatuTrans, ModTrans, Puerto,
                                        EspTrans, CodTrans, CondEnvio, CodTPV, CodTienda, Estado,
                                        FechaCreacion, ServTrans, DatosLineas);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_CabCarga_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_CabCarga_Liquidar(string Usuario,
                                         string password,
                                         string NumCarga,
                                         string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            MiddleWareTPVCentral.Utilidades.GenerarError("pasa por aquí", "TPV_CabCarga_Liquidar()", "");
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.CabCarga_Liquidar(NumCarga, Usuario, DatosLineas);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_CabCarga_Liquidar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_CabRep_Generar(string Usuario,
                                      string password,
                                      string NumeroTicket,
                                      string AlmOrigen,
                                      string AlmDestino,
                                      string FechaEnvio,
                                      string FechaReg,
                                      string FechaDoc,
                                      string AlmTransito,
                                      string Vendedor,     
                                      string NumBultos,
                                      string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            //string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            //string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            //System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            //fich.Write(DatosLineasStr);
            //fich.Close();

            //Cargo el dataset a partir del fichero temporal
            //DataSet DatosLineas = new DataSet();
            //DatosLineas.ReadXml(nomFich);

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            //if (System.IO.File.Exists(nomFich))
            //    System.IO.File.Delete(nomFich);

            //MiddleWareTPVCentral.Utilidades.GenerarError("pasa por aquí", "TPV_RegDiario()", "");
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.CabRep_Generar(NumeroTicket, AlmOrigen, AlmDestino, FechaEnvio, FechaReg,
                                FechaDoc, AlmTransito, Vendedor, NumBultos, DatosLineas);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario + '-' + NumeroTicket, "TPV_CabRep_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cheques_Central(string Usuario, string password, string NumDoc)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }


            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Cheques_Central(NumDoc);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cheques_Central()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cheques_CentralArti(string Usuario, string password, string NumDoc)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }


            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Cheques_CentralArti(NumDoc);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cheques_CentralArti()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cierre_Tienda_TPV(string Usuario,
                                         string password,
                                         string CodTienda,
                                         string CodTPV,
                                         string FechaIni,
                                         string HoraIni,
                                         string FechaFin,
                                         string HoraFin,
                                         string Activo,
                                         string Registrado,
                                         string DifCaja,
                                         string Recuento,
                                         string SaldoFin,
                                         string TotTrasp,
                                         string TotGastos,
                                         string TotCambio,
                                         string Turno)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cierre_Tienda_TPV(CodTienda, CodTPV, FechaIni, HoraIni, FechaFin, HoraFin,
                                           Activo, Registrado, DifCaja, Recuento, SaldoFin, TotTrasp,
                                           TotGastos, TotCambio, Turno);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cierre_Tienda_TPV()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cliente_Contador_NIF(string Usuario, string password, string NIF)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cliente_Contador_NIF(NIF);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Contador_NIF()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cliente_Contador_Nombre(string Usuario, string password, string Nombre)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cliente_Contador_Nombre(Nombre);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Contador_Nombre()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cliente_Contador_PorCodigo(string Usuario, string password, string CodCliente)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cliente_Contador_Codigo(CodCliente);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Ficha_PorCodigo()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cliente_Contador_Tlfno(string Usuario, string password, string Telefono)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cliente_Contador_Tlfno(Telefono);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Contador_Tlfno()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cliente_Ficha_NIF(string Usuario, string password, string NIF)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cliente_Ficha_NIF(NIF);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Ficha_NIF()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cliente_Ficha_Nombre(string Usuario, string password, string Nombre)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cliente_Ficha_Nombre(Nombre);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Ficha_Nombre()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cliente_Ficha_PorCodigo(string Usuario, string password, string CodCliente)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cliente_Ficha_PorCodigo(CodCliente);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Ficha_PorCodigo()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Update_Customer(string Usuario, string password, string CustCode)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Update_Customer(CustCode);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario, "TPV_Update_Customer()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Cliente_Ficha_Tlfno(string Usuario, string password, string Telefono)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Cliente_Ficha_Tlfno(Telefono);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Cliente_Ficha_Tlfno()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Competidores(string Usuario,
                                    string password,
                                    string CodProv,
                                    string Nombre,
                                    string Direccion,
                                    string Direccion2,
                                    string Poblacion,
                                    string Pais,
                                    string CP,
                                    string Provincia,
                                    string Mail,
                                    string Web,
                                    string Telefono,
                                    string Fax,
                                    string FechaMod,
                                    string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            MiddleWareTPVCentral.Utilidades.GenerarError("pasa por aquí", "TPV_Competidores()", "");
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Competidores(CodProv, Nombre, Direccion, Direccion2, Poblacion,
                                        Pais, CP, Provincia, Mail, Web, Telefono,
                                        Fax, FechaMod, DatosLineas);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Competidores()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Comprobar_Apertura_Tienda(string Usuario,
                                                 string password,
                                                 string CodTienda,
                                                 string FechaApertura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Comprobar_Apertura_Tienda(CodTienda, FechaApertura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Comprobar_Apertura_Tienda()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Consulta_Stock(string Usuario,
                                      string password,
                                      string NumeroProducto,
                                      string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Producto_Stock(NumeroProducto, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Consulta_Stock()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Consulta_Stock_Variante(string Usuario,
                                               string password,
                                               string NumeroProducto,
                                               string DatosLineasStr,
                                               string DatosMultiStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich1 = Cx_Log1 + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(DatosMultiStr);
            fich1.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(nomFich1);

            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Producto_Stock_Variante(NumeroProducto, DatosLineas, DatosMulti);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Consulta_Stock_Variante()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_DevEfectivo(string Usuario,
                                   string password,
                                   string Ticket,
                                   string Inicio,
                                   string CodForma,
                                   string Imp,
                                   string TipoPago,
                                   string CodTPV,
                                   string CodTienda,
                                   string TipoVale,
                                   string FechaReg,
                                   string NumCli,
                                   string NumTurno,
                                   string CodVendedor)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }


            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.DevEfectivo(Ticket, Inicio, CodForma, Imp, TipoPago, CodTPV, CodTienda,
                                   TipoVale, FechaReg, NumCli, NumTurno, CodVendedor);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_DevEfectivo()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Devolver_TPV_Activos(string Usuario,
                                            string password,
                                            string CodTienda,
                                            string FechaApertura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Devolver_TPV_Activos(CodTienda, FechaApertura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Devolver_TPV_Activos()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Devolver_Vendedores(string Usuario,
                                           string password,
                                           string CodTienda,
                                           string FechaApertura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Devolver_Vendedores(CodTienda, FechaApertura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Devolver_Vendedores()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_DiarioProd_Generar(string Usuario, string password, string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }
            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();
            
            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);
            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            //MiddleWareTPVCentral.Utilidades.GenerarError("pasa por aquí", "TPV_RegDiario()", "");
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.DiarioProd_Generar(DatosLineas);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_DiarioProd_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Entrega_Generar(string Usuario,
                                       string password,
                                       string NumeroTicket,
                                       string Cliente,
                                       string CodTienda,
                                       string CodAlmacen,
                                       string CodDirEnvio,
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
                                       string TotalApagar,
                                       string ImporteVenta,
                                       string ImporteEntregado,
                                       string NumeroReserva,
                                       string ImportePago,
                                       string CtaContrapartida,
                                       string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }
            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            //string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich1 = Cx_Log1 + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";

            //System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            //fich1.Write(DatosMultiStr);
            //fich1.Close();

            ////Cargo el dataset a partir del fichero temporal
            //DataSet DatosMulti = new DataSet();
            //DatosMulti.ReadXml(nomFich1);


            //if (System.IO.File.Exists(nomFich1))
            //    System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Entregas_Crear(NumeroTicket, Cliente, CodTienda, CodDirEnvio, DatosLineas,
                EnvioNombre, EnvioDireccion, EnvioCP, EnvioPoblacion, EnvioProvincia, EnvioAtencion,
                CodFormaPago, CodVendedor, FechaTicket, CodTPV, HoraTicket, TotalApagar, ImporteVenta,
                ImporteEntregado, NumeroReserva, ImportePago, CtaContrapartida, CodAlmacen);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Entrega_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Factura_Generar(string Usuario,
                                       string password,
                                       string NumeroTicket,
                                       string Cliente,
                                       string CodTienda,
                                       string CodDirEnvio,
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
                                       string TotalApagar,
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
                                       string DatosLineasStr,
                                       string DatosMultiStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];            
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich1 = Cx_Log1 + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(DatosMultiStr);
            fich1.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(nomFich1);


            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));

            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(new System.IO.StringReader(DatosMultiStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Facturas_Crear(NumeroTicket, Cliente, CodTienda, CodDirEnvio, DatosLineas,
                EnvioNombre, EnvioDireccion, EnvioCP, EnvioPoblacion, EnvioProvincia, EnvioAtencion,
                CodFormaPago, CodVendedor, FechaTicket, CodTPV, HoraTicket, TotalApagar, ImporteVenta,
                ImporteEntregado, NumeroReserva, AplicarDto, NumTarjetaDto, Financiera, GruDtoCol,
                NumAutFinan, ImpGtoFinan, NumFidelizacion, CrearEntrega, CobroTrans,
                FactAuto, RepInmediata, DesColectivo, NumFacAbonar, EnvioObserv, ImpCobrarEntregas,
                ImpLiqAnticipo, FechaEnvio, DatosMulti);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario + "-" + NumeroTicket, "TPV_Factura_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_FinDiaArqueo(string Usuario, string password, string CodTienda,
                                  string CodTPV, string FechaIni, string HoraIni, string FechaFin,
                                  string HoraFin, string Activo, string Registrado, string TotGastos,
                                  string TotCambio, string Turno, string Vendedor, string Arqueo,
                                  string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.FinDiaArqueo(CodTienda, CodTPV, FechaIni, HoraIni, FechaFin, HoraFin,
                                           Activo, Registrado, TotGastos, TotCambio, Turno,
                                           Vendedor, Arqueo, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_FinDiaArqueo()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_FinDiaCaja(string Usuario, string password, string CodTienda,
                                  string CodTPV, string FechaIni, string HoraIni, string FechaFin,
                                  string HoraFin, string Activo, string Registrado, string TotGastos,
                                  string TotCambio, string Turno, string Vendedor, string NumDoc,
                                  string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.FinDiaCaja(CodTienda, CodTPV, FechaIni, HoraIni, FechaFin, HoraFin,
                                           Activo, Registrado, TotGastos, TotCambio, Turno,
                                           Vendedor, NumDoc, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_FinDiaCaja()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_FinDiaCaja_Full(string Usuario,
                                       string password,
                                       string CodTienda,
                                       string CodTPV,
                                       string FechaIni,
                                       string HoraIni,
                                       string FechaFin,
                                       string HoraFin,
                                       string Activo,
                                       string Registrado,
                                       string TotGastos,
                                       string TotCambio,
                                       string Turno,
                                       string Vendedor,
                                       string NumDoc,
                                       string SaldoInicial,
                                       string TotalVentas,
                                       string TotalCobros,
                                       string TotalPagos,
                                       string TotalDevolucionesCaja,
                                       string TotalRedondeos,
                                       string TotalVentasDia,
                                       string DiferenciaCaja,
                                       string FechaUltModificacion,
                                       string TotalRecuento,
                                       string TotalEntregasACta,
                                       string TotCobrosEfectivo,
                                       string TotPagosEfectivo,
                                       string TotVentasEfectivo,
                                       string TotalTarjetas,
                                       string TotalOtrasFormasDePago,
                                       string TotalTraspasoAFilial,
                                       string TotalVentasAbonoDto,
                                       string TotalEntregasCtaEfect,
                                       string TotalDevolucionesTarjeta,
                                       string TotalVales,
                                       string TotalValesRecibidos,
                                       string TotalEntregasCtaTarjeta,
                                       string Apertura,
                                       string SaldoFinalTPV,
                                       string VendedorCaja,
                                       string ReservasCanceladasEfectivo,
                                       string ReservasCanceladasCuenta,
                                       string CodArqueoDeCierre,
                                       string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.FinDiaCajaFull(
                           CodTienda,               // 1
                           CodTPV,
                           FechaIni,
                           HoraIni,
                           FechaFin,                // 5
                           HoraFin,
                           Activo,
                           Registrado,
                           TotGastos,
                           TotCambio,               // 10
                           Turno,
                           Vendedor,
                           NumDoc,
                           SaldoInicial,
                           TotalVentas,             // 15
                           TotalCobros,
                           TotalPagos,
                           TotalDevolucionesCaja,
                           TotalRedondeos,
                           TotalVentasDia,          // 20
                           DiferenciaCaja,
                           FechaUltModificacion,
                           TotalRecuento,
                           TotalEntregasACta,
                           TotCobrosEfectivo,            // 25
                           TotPagosEfectivo,
                           TotVentasEfectivo,
                           TotalTarjetas,
                           TotalOtrasFormasDePago,
                           TotalTraspasoAFilial,         // 30
                           TotalVentasAbonoDto,
                           TotalEntregasCtaEfect,
                           TotalDevolucionesTarjeta,
                           TotalVales,
                           TotalValesRecibidos,           // 35
                           TotalEntregasCtaTarjeta,
                           Apertura,
                           SaldoFinalTPV,
                           VendedorCaja,
                           ReservasCanceladasEfectivo,  // 40
                           ReservasCanceladasCuenta,
                           CodArqueoDeCierre,
                           DatosLineas);                // 43
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_FinDiaCaja_Full()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Get_Reaprovision_Tienda_Pendiente_DiaProd(string Usuario, string password, string tienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Get_Reaprovision_Tienda_Pendiente_DiaProd(tienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Get_Reaprovision_Tienda_Pendiente_DiaProd()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Get_Reaprovision_Tienda_Pendiente_Ind(string Usuario, string password, string tienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Get_Reaprovision_Tienda_Pendiente_Ind(tienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Get_Reaprovision_Tienda_Pendiente_Ind()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPVGrabarCabeceraTarjetaRegalo(string Usuario,
                                                   string password,
                                                   string NumeroTicket,
                                                   string Cliente,
                                                   string CodTienda,
                                                   string CodTPV,
                                                   string EnvioNombre,
                                                   string EnvioDireccion,
                                                   string EnvioCP,
                                                   string EnvioPoblacion,
                                                   string EnvioProvincia,
                                                   string EnvioAtencion,
                                                   string CodFormaPago,
                                                   string CodVendedor,
                                                   string FechaTicket,
                                                   string CrearEntrega,
                                                   string AplicarDto,
                                                   string NumTarjetaDto,
                                                   string GruDtoCol,
                                                   string NumFidelizacion,
                                                   string CobroTrans,
                                                   string FactAuto,
                                                   string RepInmediata,
                                                   string DesColectivo,
                                                   string AlmDestino,
                                                   string EnvioObserv,
                                                   string NumLinPago,
                                                   string CodVenAnticipo,
                                                   string FechaEnvio,
                                                   string ActTRegalo,
                                                   string DesactTRegalo,
                                                   string DatosLineasStr,
                                                   string DatosMultiStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            //PREPARO EL XML CON LOS DATOS DE LAS MULTIFORMAS DE PAGO
            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich1 = Cx_Log1 + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(DatosMultiStr);
            fich1.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(nomFich1);


            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));

            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(new System.IO.StringReader(DatosMultiStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.GrabarCabeceraTarjetaRegalo(NumeroTicket, Cliente, CodTienda, CodTPV,
                EnvioNombre, EnvioDireccion, EnvioCP, EnvioPoblacion, EnvioProvincia, EnvioAtencion,
                CodFormaPago, CodVendedor, FechaTicket, CrearEntrega, GruDtoCol,
                NumFidelizacion, CobroTrans, FactAuto, RepInmediata, DesColectivo,
                AlmDestino, EnvioObserv, NumLinPago, CodVenAnticipo, FechaEnvio,
                ActTRegalo, DesactTRegalo, DatosLineas, DatosMulti);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPVGrabarCabeceraTarjetaRegalo()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_HistCambioTarifa(string Usuario,
                                        string password,
                                        string CodProducto,
                                        string UDS,
                                        string CodTarifa,
                                        string PreVenta,
                                        string Coste,
                                        string FechaIni,
                                        string FechaFin,
                                        string VendSol,
                                        string VendResol,
                                        string Motivo,
                                        string MargenAct,
                                        string ImpAct,
                                        string UltPrecVenta,
                                        string FechaSol,
                                        string MargenSol,
                                        string ImpSol,
                                        string FechaResol,
                                        string Estado,
                                        string Resolucion,
                                        string CodTienda,
                                        string CodTpv)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.HistCambioTarifa(CodProducto, UDS, CodTarifa, PreVenta, Coste,
                                  FechaIni, FechaFin, VendSol, VendResol, Motivo, MargenAct,
                                  ImpAct, UltPrecVenta, FechaSol, MargenSol, ImpSol, FechaResol,
                                  Estado, Resolucion, CodTienda, CodTpv);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_HistCambioTarifa()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_HistLinFra_Generar(string Usuario, string password, string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.HistLinFra_Generar(DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_HistLinFra_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Incidencia_Generar(string Usuario,
                                          string password,
                                          string NoIncidencia,
                                          string Origen,
                                          string Destino,
                                          string FechaRegistro,
                                          string HoraRegistro,
                                          string CodTienda,
                                          string CodTPV,
                                          string TipoIncidencia,
                                          string CodCliente,
                                          string NombreCliente,
                                          string ApellidosCliente,
                                          string CodVendedorRegistro,
                                          string NPedidoTransf,                                          
                                          string NoTicket,
                                          string TPVSettle,
                                          string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();            
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Incidencias_Crear(NoIncidencia, Origen, Destino,
                                         FechaRegistro, HoraRegistro, CodTienda, CodTPV, TipoIncidencia, CodCliente, NombreCliente,
                                         ApellidosCliente, CodVendedorRegistro, NPedidoTransf, NoTicket, TPVSettle, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario + "-" + NoIncidencia, "TPV_Incidencia_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_LinIncidencia_Update(string User,
                                            string Password,
                                            string NoIncidencia,
                                            string LineNo,
                                            string ShippingDate,
                                            string ShippingTime,
                                            string ShopComment,
                                            string Status)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((User != null) && (Password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(User, Password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }


            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.LinIncidencia_Update(NoIncidencia, LineNo, User, ShippingDate, ShippingTime, ShopComment, Status);
                                                                                  
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(User + "-" + NoIncidencia, "TPV_LinIncidencia_Update()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_MarkShipmentNotReceived(string Usuario,
                                               string password,
                                               string codTienda,
                                               string fechaInicio,
                                               string fechaFin)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.MarkShipmentNotReceived(codTienda, fechaInicio, fechaFin);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario, "TPV_Sincronizar_Transferencias()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Mod_Cliente_Factura(string Usuario,
                                           string password,
                                           string Numero,
                                           string Nombre,
                                           string Nombre2,
                                           string Direccion,
                                           string Cp,
                                           string Poblacion,
                                           string Provincia,
                                           string NIF,
                                           string NumFactura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Mod_Cliente_Factura(Numero, Nombre, Nombre2, Direccion, Cp, Poblacion,
                                           Provincia, NIF, NumFactura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Mod_Cliente_Factura()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obt_EntregaCarga(string Usuario, string password, string FechaDesde, string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obt_EntregaCarga(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obt_EntregaCarga()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_ObtenerFechaUltApertura(string Usuario, string password, string CodTienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.ObtenerFechaUltApertura(CodTienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_ObtenerFechaUltApertura()", ex.Message);

        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Diario_Producto(string Usuario, string password, string tienda, string seccion)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Diario_Producto(tienda, seccion);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Diario_Producto()", ex.Message);
        }
    }

    //BYL: Escribe en Central el resultado de ejecuciones de ajustes de inventario en TPV
    [WebMethod(true)]
    public DataSet TPV_Resultado_Ajuste_Inventario(string Usuario, string password, string numero, string tienda, string seccion, bool resultado, string ErrorMsg)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Resultado_Ajuste_Inventario(numero, tienda, seccion, resultado, ErrorMsg);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Resultado_Ajuste_Inventario()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Entregas_Estado(string Usuario, string password, string tienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Entregas_Estado(tienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Entregas_Estado()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Entregas_Tienda(string Usuario, string password, string tienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Entregas_Tienda(tienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Entregas_tienda()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Factura(string Usuario, string password, string NumFactura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Factura(NumFactura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Factura()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_HistTarifas(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_HistTarifas();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_HistTarifas()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Multiformas(string Usuario, string password, string NumFactura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Multiformas(NumFactura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Multiformas()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_NotaEntrega(string Usuario, string password, string NumNotaEntrega)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_NotaEntrega(NumNotaEntrega);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_NotaEntrega()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_PteFactura(string Usuario, string password, string NumFactura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_PteFactura(NumFactura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_PteFactura()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Reaprovision_Tienda(string Usuario, string password, string tienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Reaprovision_Tienda(tienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Reaprovision_tienda()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Reaprovision_Tienda_Independiente(string Usuario, string password, string tienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Reaprovision_Tienda_Independiente(tienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Reaprovision_Tienda_Independiente()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Reaprovision_Tienda_Independiente_Por_Documento(string Usuario,
                                                                               string password,
                                                                               string tienda,
                                                                               string noDocumento)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Reaprovision_Tienda_Independiente(tienda, noDocumento, true);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Reaprovision_Tienda_Independiente_Por_Documento()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Reaprovision_Tienda_Por_Documento(string Usuario,
                                                                 string password,
                                                                 string tienda,
                                                                 string noDocumento)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Reaprovision_Tienda(tienda, noDocumento, true);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Reaprovision_Tienda_Por_Documento()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Reposicion_Tienda(string Usuario, string password, string tienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Reposicion_Tienda(tienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Reposicion_tienda()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Reposicion_Tienda_Lineas(string Usuario, string password, string Numero)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Reposicion_Tienda_Lineas(Numero);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Reposicion_Tienda_Lineas()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Obtener_Reserva(string Usuario, string password, string NumFactura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Obtener_Reserva(NumFactura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Obtener_Reserva()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_PedCompra_Generar(string Usuario,
                                         string password,
                                         string NumeroTicket,
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
                                         string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.PedCompra_Generar(NumeroTicket, Proveedor, CodTienda,
                EnvioNombre, EnvioDireccion, EnvioCP, EnvioPoblacion, EnvioProvincia, EnvioAtencion,
                CodFormaPago, CodVendedor, FechaTicket, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_PedCompra_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_PedCompra_Registrar(string Usuario,
                                           string password,
                                           string NumeroTicket,
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
                                       string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);

            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();

            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.PedCompra_Registrar(NumeroTicket, Proveedor, CodTienda,
                EnvioNombre, EnvioDireccion, EnvioCP, EnvioPoblacion, EnvioProvincia, EnvioAtencion,
                CodFormaPago, CodVendedor, FechaTicket, NumAlbaran, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_PedCompra_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Proveedor_Contador_NIF(string Usuario, string password, string NIF)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Proveedor_Contador_NIF(NIF);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Proveedor_Contador_NIF()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Proveedor_Contador_Nombre(string Usuario, string password, string Nombre)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Proveedor_Contador_Nombre(Nombre);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Proveedor_Contador_Nombre()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Proveedor_Ficha_NIF(string Usuario, string password, string NIF)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Proveedor_Ficha_NIF(NIF);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Proveedor_Ficha_NIF()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Proveedor_Ficha_Nombre(string Usuario, string password, string Nombre)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Proveedor_Ficha_Nombre(Nombre);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Proveedor_Ficha_Nombre()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Prueba(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.PruebaDate();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Productos()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Recepcion_Transferencia_Pendiente(string Usuario, string password, string Lineas)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(Lineas);
            fich1.Close();
            DataSet LineasXML = new DataSet();
            LineasXML.ReadXml(nomFich1);
            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet LineasXML = new DataSet();
            LineasXML.ReadXml(new System.IO.StringReader(Lineas));            
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Recepcion_Transferencia_Pendiente(LineasXML);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Recepcion_Transferencia_Pendiente()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_RegDiario(string Usuario,
                                 string password,
                                 string Fecharegistro,
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
                                 string Signo,
                                 string Vendedor)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.RegDiario(Fecharegistro, Tipodocumento, NDocumento, TipoMovimiento,
                                       NCuenta, VDescripcion, VImporteTex, TipoContrapartidaTex,
                                       NContrapartida, CodTienda, CodTPV, NTurnoTex,
                                       TipoMovTraspasoTex, Signo, Vendedor);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario + " - " + NDocumento, "TPV_RegDiario()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Reg_Transferencia(string Usuario,
                                         string password,
                                         string NumeroTransferencia,
                                         string Vendedor,
                                         string Cliente,
                                         string NombreCli,
                                         string MotivoDev,
                                         string Comentario,
                                         string NLinea,
                                         string DatosLineasStr,
                                         string DatosMultiStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }
            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich1 = Cx_Log1 + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(DatosMultiStr);
            fich1.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(nomFich1);


            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));

            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(new System.IO.StringReader(DatosMultiStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Reg_Transferencia(NumeroTransferencia, Vendedor, Cliente, NombreCli, MotivoDev, Comentario, NLinea, DatosLineas, DatosMulti);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario + "-" + NumeroTransferencia, "TPV_Reg_Transferencia()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Registrar_Asistencia(string Usuario,
                                            string password,
                                            string Tienda,
                                            string Fecha,
                                            string Vendedor,
                                            string Hora,
                                            string GrupoVenta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Registrar_Asistencia(Tienda, Fecha, Vendedor, Hora, GrupoVenta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Registrar_Asistencia()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Registrar_CierreTienda(string Usuario,
                                              string password,
                                              string Tienda,
                                              string Fecha,
                                              string Vendedor,
                                              string Hora)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Registrar_CierreTienda(Tienda, Fecha, Vendedor, Hora);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Registrar_CierreTienda()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Reserva_Cancelar(string Usuario,
                                        string password,
                                        string Ticket,
                                        string CodTPV,
                                        string CodTienda,
                                        string DatosMultiStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            //PREPARO EL XML CON LOS DATOS DE LAS MULTIFORMAS DE PAGO
            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich1 = Cx_Log1 + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(DatosMultiStr);
            fich1.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(nomFich1);


            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(new System.IO.StringReader(DatosMultiStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Reserva_Cancelar(Ticket, CodTPV, CodTienda, DatosMulti);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_ReservaCancelar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Reserva_Generar(string Usuario,
                                       string password,
                                       string NumeroTicket,
                                       string Cliente,
                                       string CodTienda,
                                       string CodTPV,
                                       string EnvioNombre,
                                       string EnvioDireccion,
                                       string EnvioCP,
                                       string EnvioPoblacion,
                                       string EnvioProvincia,
                                       string EnvioAtencion,
                                       string CodFormaPago,
                                       string CodVendedor,
                                       string FechaTicket,
                                       string CrearEntrega,
                                       string AplicarDto,
                                       string NumTarjetaDto,
                                       string GruDtoCol,
                                       string NumFidelizacion,
                                       string CobroTrans,
                                       string FactAuto,
                                       string RepInmediata,
                                       string DesColectivo,
                                       string AlmDestino,
                                       string EnvioObserv,
                                       string NumLinPago,
                                       string CodVenAnticipo,
                                       string FechaEnvio,
                                       string DatosLineasStr,
                                       string DatosMultiStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            //PREPARO EL XML CON LOS DATOS DE LAS MULTIFORMAS DE PAGO
            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich1 = Cx_Log1 + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(DatosMultiStr);
            fich1.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(nomFich1);


            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));

            DataSet DatosMulti = new DataSet();
            DatosMulti.ReadXml(new System.IO.StringReader(DatosMultiStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Reservas_Crear(NumeroTicket, Cliente, CodTienda, CodTPV,
                EnvioNombre, EnvioDireccion, EnvioCP, EnvioPoblacion, EnvioProvincia, EnvioAtencion,
                CodFormaPago, CodVendedor, FechaTicket, CrearEntrega,
                AplicarDto, NumTarjetaDto, GruDtoCol,
                NumFidelizacion, CobroTrans, FactAuto, RepInmediata, DesColectivo,
                AlmDestino, EnvioObserv, NumLinPago, CodVenAnticipo, FechaEnvio,
                DatosLineas, DatosMulti);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Reserva_Generar()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Banco(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Banco();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Banco()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_CargoProducto(string Usuario, string password, string CodCargo)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_CargoProducto(CodCargo);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_CargoProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_CentroResponsabilidad(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_CentroResponsabilidad(Usuario);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_CentroResponsabilidad()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Cliente(string Usuario,
                                           string password,
                                           string FechaDesde,
                                           string FechaHasta,
                                           string CodCliGen)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Clientes(FechaDesde, FechaHasta, CodCliGen);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Cliente()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_CodificacionErroresTR(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_CodificacionErroresTR();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_CodificacionErroresTR()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Coleccion(string Usuario,
                                             string password,
                                             string FechaDesde,
                                             string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Coleccion(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Coleccion()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Color(string Usuario, string password, string FechaDesde, string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Color(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Color()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_ComisionVendedor(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_ComisionVendedor();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_ComisionVend()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Composicion(string Usuario,
                                               string password,
                                               string FechaDesde,
                                               string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Composicion(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Composicion()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_ConfGrContable(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_ConfGrContable();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_ConfGrContable()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_ConfGrIVA(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_ConfGrIVA();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_ConfGrIVA()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Cuentas(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Cuentas();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Cuentas()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_DtoLinea(string Usuario,
                                            string password,
                                            string FechaDesde,
                                            string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_DtoLinea(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_DtoLinea()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Empleados(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Empleados(Usuario);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Empleados()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_FamProd(string Usuario,
                                           string password,
                                           string FechaDesde,
                                           string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_FamProd(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_FamProd()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Familia(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Familia();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Familia()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_FormasPago(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_FormasPago();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_FormasPago()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrContableNegocio(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrContableNegocio();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrContableNegocio()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrContableProducto(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrContableProducto();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrContableProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrIVANegocio(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrIVANegocio();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrIVANegocio()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrIVAProducto(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrIVAProducto();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrIVAProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrupoContableBanco(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrupoContableBanco();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrupoContableBanco()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrupoContableCliente(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrupoContableCliente();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrupoContableCliente()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrupoContableExistencias(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrupoContableExistencias();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrupoContableExistencias()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrupoContableProveedor(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrupoContableProveedor();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrupoContableProveedor()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrupoDescuentoCliente(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrupoDescuentoCliente();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrupoDescuentoCliente()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrupoDescuentoProducto(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrupoDescuentoProducto();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrupoDescuentoProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrupoPrecioCliente(string Usuario, string password, string CodTarifa)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrupoPrecioCliente(CodTarifa);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrupoPrecioCliente()", ex.Message);
        }
    }

    //DEPRECATED
    [WebMethod(true)]
    public DataSet TPV_Sincronizar_GrupoVentas(string Usuario, string password)
    {
        return new DataSet();
        /*
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_GrupoVentas();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrupoVentas()", ex.Message);
        }
        */
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_HistLinFactura(string Usuario, string password, string NumFactura)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_HistLinFactura(NumFactura);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_HistLinFactura()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_LinProducto(string Usuario,
                                               string password,
                                               string FechaDesde,
                                               string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_LinProducto(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_LinProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Motivo_Devolucion(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Motivo_Devolucion(Usuario);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Motivo_Devolucion()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_PedCompra(string Usuario, string password, string CodTienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_PedCompra(CodTienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_PedCompra()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_PrimaProducto(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_PrimaProducto();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_PrimaProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_PrimaProducto_Dataport(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_PrimaProducto_Dataport();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_PrimaProducto_Dataport()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_ComisionVendedor_Dataport(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_ComisionVendedor_Dataport();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_ComisionVendedor_Dataport()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Productos_Dataport(string Usuario,
                                                      string password,
                                                      string FechaDesde,
                                                      string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Productos_Dataport(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Productos_Dataport()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Variantes_Dataport(string Usuario,
                                                      string password,
                                                      string FechaDesde,
                                                      string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Variantes_Dataport(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario, "TPV_Sincronizar_Variantes_Dataport()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_ProdTallaColor(string Usuario,
                                                  string password,
                                                  string FechaDesde,
                                                  string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_ProdTallaColor(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_ProdTallaColor()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Productos(string Usuario,
                                             string password,
                                             string FechaDesde,
                                             string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Productos(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Productos()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_ProductosPack(string Usuario,
                                                 string password,
                                                 string FechaDesde,
                                                 string FechaHasta,
                                                 string CodTienda)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_ProductosPack(FechaDesde, FechaHasta, CodTienda);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_ProductosPack()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Productos_FiltroProducto(string Usuario,
                                                            string password,
                                                            string FechaDesde,
                                                            string FechaHasta,
                                                            string FiltroProducto)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Productos(FechaDesde, FechaHasta, FiltroProducto);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Productos_FiltroProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Recursos(string Usuario,
                                            string password,
                                            string FechaDesde,
                                            string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Recursos(Usuario, FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Recursos()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_SubFamProd(string Usuario,
                                              string password,
                                              string FechaDesde,
                                              string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_SubFamProd(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_SubFamProd()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_SubFamilia(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_SubFamilia();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_SubFamilia()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Tallaje(string Usuario,
                                           string password,
                                           string FechaDesde,
                                           string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Tallaje(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Tallaje()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_TarifasProducto(string Usuario,
                                                   string password,
                                                   string FechaDesde,
                                                   string FechaHasta,
                                                   string CodTarifa,
                                                   string PrecioIni)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_TarifaVentaProducto(FechaDesde, FechaHasta, CodTarifa, PrecioIni);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_TarifasProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_TarifasProducto_Dataport(string Usuario,
                                                            string password,
                                                            string FechaDesde,
                                                            string FechaHasta,
                                                            string CodTarifa,
                                                            string PrecioIni)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_TarifaVentaProducto_Dataport(FechaDesde, FechaHasta, CodTarifa, PrecioIni);
        }
        catch (Exception ex)
        {

            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_TarifasProducto_Dataport()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_TarifasProducto_FiltroProducto(string Usuario,
                                                                  string password,
                                                                  string FechaDesde,
                                                                  string FechaHasta,
                                                                  string CodTarifa,
                                                                  string PrecioIni,
                                                                  string FiltroProducto)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_TarifaVentaProducto(FechaDesde, FechaHasta, CodTarifa, PrecioIni, FiltroProducto);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_TarifasProducto_FiltroProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Temporada(string Usuario,
                                             string password,
                                             string FechaDesde,
                                             string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Temporada(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Temporada()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Tickets(string Usuario,
                                           string password,
                                           string codTienda,
                                           string TicketsDia)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }            

            /*
            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(TicketsDia);
            fich1.Close();
            DataSet TicketsDiaXML = new DataSet();
            TicketsDiaXML.ReadXml(nomFich1);
            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet TicketsDiaXML = new DataSet();
            TicketsDiaXML.ReadXml(new System.IO.StringReader(TicketsDia));
            //BYL: Eliminación fichero temporal
            
            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Sincronizar_Tickets_Sin_Integridad(codTienda, TicketsDiaXML);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario, "TPV_Sincronizar_Tickets()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Tickets_Con_Integridad(string Usuario,
                                                          string password,
                                                          string codTienda,
                                                          string TicketsDia)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            /*
            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(TicketsDia);
            fich1.Close();
            DataSet TicketsDiaXML = new DataSet();
            TicketsDiaXML.ReadXml(nomFich1);
            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet TicketsDiaXML = new DataSet();
            TicketsDiaXML.ReadXml(new System.IO.StringReader(TicketsDia));
            //BYL: Eliminación fichero temporal

            return TPV.Sincronizar_Tickets_Con_Integridad(codTienda, TicketsDiaXML);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(Usuario, "TPV_Sincronizar_Tickets_Con_Integridad()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Tiendas(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Tiendas();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Tiendas()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Tiendas_Dataport(string Usuario, string password)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Tiendas_Dataport();
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Tiendas_Dataport()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Transferencias(string Usuario,
                                                  string password,
                                                  string codTienda,
                                                  string Transferencias)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            /*
            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(Transferencias);
            fich1.Close();
            DataSet TransferenciasXML = new DataSet();
            TransferenciasXML.ReadXml(nomFich1);
            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);
            */

            //BYL: Eliminación fichero temporal
            DataSet TransferenciasXML = new DataSet();
            TransferenciasXML.ReadXml(new System.IO.StringReader(Transferencias));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Sincronizar_Transferencias(codTienda, TransferenciasXML);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Transferencias()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Transferencias_Entre_Fechas(string Usuario,
                                                               string password,
                                                               string codTienda,
                                                               string fechaInicio,
                                                               string fechaFin,
                                                               string Transferencias)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            string Cx_Log1 = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            string nomFich1 = Cx_Log1 + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";
            System.IO.StreamWriter fich1 = new System.IO.StreamWriter(nomFich1, false);
            fich1.Write(Transferencias);
            fich1.Close();
            DataSet TransferenciasXML = new DataSet();
            TransferenciasXML.ReadXml(nomFich1);
            if (System.IO.File.Exists(nomFich1))
                System.IO.File.Delete(nomFich1);

            return TPV.Sincronizar_Transferencias_Entre_Fechas(codTienda, fechaInicio, fechaFin, TransferenciasXML);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Transferencias()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Variante(string Usuario,
                                            string password,
                                            string FechaDesde,
                                            string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Variantes(FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Variante()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Variante_FiltroProducto(string Usuario,
                                                           string password,
                                                           string FechaDesde,
                                                           string FechaHasta,
                                                           string FiltroProducto)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Variantes(FechaDesde, FechaHasta, FiltroProducto);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Variante_FiltroProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Sincronizar_Vendedores(string Usuario,
                                              string password,
                                              string FechaDesde,
                                              string FechaHasta)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_Vendedores(user.UserCode, FechaDesde, FechaHasta);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_Vendedores()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Subir_ArqueoCaja(string Usuario,
                                        string password,
                                        string CabCodArq,
                                        string CabCodTienda,
                                        string CabCodTPV,
                                        string CabFecha,
                                        string CabNumTurno,
                                        string CabHora,
                                        string CabCodVendedor,
                                        string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }
            /*
            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);
            */

            //BYL: Eliminación fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(new System.IO.StringReader(DatosLineasStr));
            //BYL: Eliminación fichero temporal

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Subir_ArqueoCaja(CabCodArq, CabCodTienda, CabCodTPV, CabFecha, CabNumTurno,
                                        CabHora, CabCodVendedor, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Subir_ArqueoCaja()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Subir_JefesSecc(string Usuario,
                                       string password,
                                       string CodTienda,
                                       string vendedor,
                                       string hora,
                                       string fecha,
                                       string TiendaAbierta,
                                       string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Subir_JefesSecc(CodTienda, vendedor, hora, fecha, TiendaAbierta, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Subir_JefesSecc()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_TarifaAdmon(string Usuario,
                                   string password,
                                   string Codigo,
                                   string VenAdmon,
                                   string Estado,
                                   string FAdmon,
                                   string ValDir,
                                   string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.TarifaAdmon(Codigo, VenAdmon, Estado, FAdmon, ValDir, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_TarifaAdmon()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_TarifaDireccion(string Usuario,
                                       string password,
                                       string Codigo,
                                       string VenDir,
                                       string Estado,
                                       string FDir,
                                       string ValDir,
                                       string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.TarifaDireccion(Codigo, VenDir, Estado, FDir, ValDir, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_TarifaDireccion()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_TarifaSolicitud(string Usuario,
                                       string password,
                                       string Codigo,
                                       string CodTienda,
                                       string CodTPV,
                                       string FechaCre,
                                       string HoraCre,
                                       string Vendedor,
                                       string Etiqueta,
                                       string FIni,
                                       string FFin,
                                       string Tarifa,
                                       string ActCostes,
                                       string DatosLineasStr)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string nomFich = Cx_Log + DateTime.Now.ToString("ddMMyyyyHHmmssdd") + ".xml";
            string nomFich = Cx_Log + MiddleWareTPVCentral.Utilidades.GenerarNombreFichero() + ".xml";

            System.IO.StreamWriter fich = new System.IO.StreamWriter(nomFich, false);
            fich.Write(DatosLineasStr);
            fich.Close();

            //Cargo el dataset a partir del fichero temporal
            DataSet DatosLineas = new DataSet();
            DatosLineas.ReadXml(nomFich);

            if (System.IO.File.Exists(nomFich))
                System.IO.File.Delete(nomFich);

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.TarifaSolicitud(Codigo, CodTienda, CodTPV, FechaCre, HoraCre, Vendedor, Etiqueta,
                                       FIni, FFin, Tarifa, ActCostes, DatosLineas);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_TarifaSolicitud()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_TarifasProd_PrecioInicio(string Usuario,
                                                string password,
                                                string FechaDesde,
                                                string FechaHasta,
                                                string PrecioIni)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
            return TPV.Sincronizar_TarifasProdPrecioIni(FechaDesde, FechaHasta, PrecioIni);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_TarifasProd_PrecioIni()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Traer_CosteProducto(string Usuario, string password, string NumProducto)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }


            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Traer_CosteProducto(NumProducto);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Traer_CosteProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Traer_LineasIncidencia(string Usuario,
                                              string password,
                                              string fechaInicio,
                                              string fechaFin)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.TraerLineasIncidencia(Usuario, fechaInicio, fechaFin);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Traer_LineasIncidencia()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Traer_Pago_Central(string Usuario, string password, string NumDoc)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }


            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Traer_Pago_Central(NumDoc);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Traer_Pago_Central()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Traer_StockProducto(string Usuario, string password, string NumProducto)
    {
        try
        {
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((Usuario != null) && (password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }


            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.Traer_StockProducto(NumProducto, Usuario);

        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Traer_StockProducto()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Visor(string FechaHoraDesde, string FechaHoraHasta)
    {
        try
        {

            return MiddleWareTPVCentral.Utilidades.VisorSucesos(Convert.ToDateTime(FechaHoraDesde), Convert.ToDateTime(FechaHoraHasta), "POLY-SQL");
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Visor()", ex.Message);
        }
    }
 

    /*
        [WebMethod(true)]
        public DataSet TPV_Sincronizar_GrContableProducto(string Usuario, string password)
        {
            try
            {
                NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
                if ((Usuario != null) && (password != null))
                {
                    DataSet DsRes = new DataSet();
                    user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
                }

                TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
                return TPV.Sincronizar_GrContableProducto();
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_GrContableProducto()", ex.Message);
            }
        }

        [WebMethod(true)]
        public DataSet TPV_Sincronizar_ProductosST(string Usuario, string password, string FechaDesde, string FechaHasta)
        {
            try
            {
                NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
                if ((Usuario != null) && (password != null))
                {
                    DataSet DsRes = new DataSet();
                    user = MiddleWareTPVCentral.Utilidades.Abrir_Login(Usuario, password, ref DsRes, WebServiceTPVCentral.Global.navConection);
                }

                TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);
                return TPV.Sincronizar_ProductosST(FechaDesde, FechaHasta);
            }
            catch (Exception ex)
            {
                return MiddleWareTPVCentral.Utilidades.GenerarError("", "TPV_Sincronizar_ProductosST()", ex.Message);
            }
        }
    */
    [WebMethod(true)]
    public DataSet TPV_Queue_Response(string User,
                                      string Password,
                                      string Id,
                                      string Result,
                                      string Fecha,
                                      string Hora,
                                      string Msg)
    {
        try
        {
            bool Result_Aux = Convert.ToBoolean(Result);
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((User != null) && (Password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(User, Password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.TPVQueueResponse(Id, Result_Aux, Fecha, Hora, Msg);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(User, "TPV_Queue_Response()", ex.Message);
        }
    }

    [WebMethod(true)]
    public DataSet TPV_Queue_Request(string User,
                                string Password, string Fecha)
    {
        try
        {            
            NavisionDB.NavisionDBUser user = new NavisionDB.NavisionDBUser();
            if ((User != null) && (Password != null))
            {
                DataSet DsRes = new DataSet();
                user = MiddleWareTPVCentral.Utilidades.Abrir_Login(User, Password, ref DsRes, WebServiceTPVCentral.Global.navConection);
            }

            TPV = new MiddleWareTPVCentral.TPV(user, WebServiceTPVCentral.Global.navConection);

            return TPV.TPVQueueRequest(Fecha);
        }
        catch (Exception ex)
        {
            return MiddleWareTPVCentral.Utilidades.GenerarError(User, "TPV_Queue_Request()", ex.Message);
        }
    }
}