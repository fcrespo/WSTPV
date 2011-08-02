using System;
using NavisionDB;
using System.IO;
using System.Data;
using System.Configuration;
using System.Net.Mail;

namespace MiddleWareTPVCentral
{
    /// <summary>
    /// Descripción breve de Utilidades.
    /// </summary>
    public class Utilidades
    {
        public Utilidades() { }

        //private NavisionDBConnection navConection = null;

        private const int LONGITUD_FILTROS = 250;
        public const string FILTRO_BLANCO = " ";        
        public static NavisionDBConnection Conectar_Navision()
        {
            string conexionLog = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            string conexionConfig = System.Configuration.ConfigurationManager.AppSettings["ConfFich"];

            NavisionDBConnection conector = new NavisionDBConnection();
            NavisionDBConfig navCfg = new NavisionDBConfig();
            StreamWriter archivo = new System.IO.StreamWriter(conexionLog);

            archivo.WriteLine(System.DateTime.Now.ToString() + " Cargando configuración...");
            if (navCfg.LoadAppSettings(conexionConfig))
            {
                conector.server = navCfg.Server;
                conector.netType = navCfg.NetType;
                conector.company = navCfg.Company;
                conector.user = navCfg.User;
                conector.password = navCfg.Password;
                conector.applicationPath = navCfg.ApplicationPath;
                conector.logFile = conexionLog;

                conector.cachesize = navCfg.cachesize;
                conector.DBname = navCfg.DBName;

                archivo.WriteLine(System.DateTime.Now.ToString() + " Configuración cargada con éxito...");
                archivo.Close();

                try
                {
                    
                    string debug = System.Configuration.ConfigurationManager.AppSettings["DEBUG_INICIO"];

                    if (debug.ToUpper() != "NO")
                    {
                        StreamWriter archi = new System.IO.StreamWriter(conexionLog, true);
                        archi.WriteLine("  Server: '" + conector.server + "'");
                        archi.WriteLine("  NetType: '" + conector.netType + "'");
                        archi.WriteLine("  Company: '" + conector.company + "'");
                        archi.WriteLine("  User: '" + conector.user + "'");
                        archi.WriteLine("  Password: '" + conector.password + "'");
                        archi.WriteLine("  ApplicationPath: '" + conector.applicationPath + "'");
                        archi.WriteLine("  cachesize: '" + conector.cachesize + "'");
                        archi.WriteLine("  logFile: '" + conector.DBname + "'");
                        archi.Close();
                    }
                    conector.open();
                }
                catch (Exception ex)
                {
                    archivo = new StreamWriter(conexionLog, true);
                    archivo.WriteLine(System.DateTime.Now.ToString() + " ERROR: " + ex.Message);
                    archivo.WriteLine(System.DateTime.Now.ToString() + " DESCONECTADO.");
                    archivo.Close();
                    conector = null;
                    throw new Exception(ex.Message);
                }
            }
            else
            {
                archivo.WriteLine(System.DateTime.Now.ToString() + " ERROR: al cargar la configuración.");
                archivo.WriteLine(System.DateTime.Now.ToString() + " DESCONECTADO.");
                archivo.Close();
                conector = null;
            }            
            return conector;
        }

        public static bool CreaFichConexion(string Servidor,
                                            string LicenciaClientePath,
                                            string Protocolo,
                                            string Compania,
                                            string Usuario,
                                            string Password)
        {
            string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];

            NavisionDBConfig NavCfg = new NavisionDBConfig();
            NavCfg.ApplicationPath = LicenciaClientePath;
            NavCfg.Server = Servidor;
            NavCfg.NetType = Protocolo;
            NavCfg.User = Usuario;
            NavCfg.Password = Password;
            NavCfg.Company = Compania;

            try
            {
                string nombreFichConf = System.Configuration.ConfigurationManager.AppSettings["ConfFich"];
                NavCfg.SaveAppSettings(nombreFichConf);
                return true;
            }
            catch (Exception ex)
            {
                System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
                LogDebug.WriteLine(DateTime.Now.ToString() + " Error al crear fichero: " + ex.Message);
                LogDebug.Close();

                string MailMsg = DateTime.Now.ToString() + " Error al crear fichero: " + ex.Message;
                string MailSubject = "WSTPV Error: CreaFichConexion()";
                SendMail(MailMsg, MailSubject);

                return false;
            }
        }

        public static DataSet GenerarResultado(string mensaje)
        {
            DataSet ds = new DataSet();
            DataTable dt = new DataTable("Resultado");

            dt.Columns.Add("Resultado");
            DataRow dr = dt.NewRow();
            dr["Resultado"] = mensaje;
            dt.Rows.Add(dr);

            ds.Tables.Add(dt);

            ds.Tables[0].AcceptChanges();
            ds.AcceptChanges();

            return ds;
        }

        public static DataSet GenerarError(string User, string fct, string mensaje)
        {
            string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];
            System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
            LogDebug.WriteLine(DateTime.Now.ToString() + " [" + User + "]: " + fct + " " + mensaje);
            LogDebug.Close();

            string MailMsg = DateTime.Now.ToString() + " [" + User + "]: " + fct + " " + mensaje;
            string MailSubject = "WSTPV Error: " + "[" + User + "]: " + fct;
            SendMail(MailMsg, MailSubject);

            DataSet ds = new DataSet();
            DataTable dt = new DataTable("Error");

            dt.Columns.Add("Error");
            DataRow dr = dt.NewRow();
            dr["Error"] = mensaje;
            dt.Rows.Add(dr);

            ds.Tables.Add(dt);

            ds.Tables[0].AcceptChanges();
            ds.AcceptChanges();

            return ds;
        }

        public static void SendMail(string Message, string Subject)
        {
            try
            {
                string MailErrors = System.Configuration.ConfigurationManager.AppSettings["MAIL_ERRORS"];
                string MailErrorsTo = System.Configuration.ConfigurationManager.AppSettings["MAIL_ERRORS_TO"];
                string SMTPServer = System.Configuration.ConfigurationManager.AppSettings["SMTP"];
            

                if ((!String.IsNullOrEmpty(MailErrors)) && (MailErrors.ToUpper() == "YES"))
                {
                    if ((!String.IsNullOrEmpty(MailErrorsTo)) && (!String.IsNullOrEmpty(SMTPServer)))
                    {
                        MailMessage Email = new MailMessage("WSTPV@bimbaylola.com", MailErrorsTo, Subject, Message);
                        SmtpClient smtpMail = new SmtpClient(SMTPServer);
                        Email.IsBodyHtml = false;
                        Email.Priority = MailPriority.High;
                        smtpMail.Send(Email);
                    }
                    else
                    {
                        if (String.IsNullOrEmpty(MailErrorsTo))
                        {
                            string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];
                            System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
                            LogDebug.WriteLine(DateTime.Now.ToString() + " [Web.Config] No existen direcciones en MAIL_ERRORS_TO de Web.Config.");
                            LogDebug.Close();
                        }
                        if (String.IsNullOrEmpty(SMTPServer))
                        {
                            string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];
                            System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
                            LogDebug.WriteLine(DateTime.Now.ToString() + " [Web.Config] No existe valor en el campo SMTP de Web.Config.");
                            LogDebug.Close();
                        }
                    }
                }
            }  
            catch (Exception ex) 
            {
                string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];
                System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
                LogDebug.WriteLine(DateTime.Now.ToString() + " [Mail Error] " + ex.Message);
                LogDebug.Close();
            }
        }

        public static DataSet GenerarErrorLeve(string User, string fct, string mensaje)
        {
            string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];
            System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
            LogDebug.WriteLine(DateTime.Now.ToString() + " [" + User + "]: " + fct + " " + mensaje);
            LogDebug.Close();

            string MailMsg = DateTime.Now.ToString() + " [" + User + "]: " + fct + " " + mensaje;
            string MailSubject = "WSTPV Error: " + "[" + User + "]: " + fct;
            SendMail(MailMsg, MailSubject);

            DataSet ds = new DataSet();
            DataTable dt = new DataTable("ErrorLeve");

            dt.Columns.Add("ErrorLeve");
            DataRow dr = dt.NewRow();
            dr["ErrorLeve"] = mensaje;
            dt.Rows.Add(dr);

            ds.Tables.Add(dt);

            ds.Tables[0].AcceptChanges();
            ds.AcceptChanges();

            return ds;
        }
        public static void Depuracion(string User, string fct, string mensaje)
        {
            string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];

            System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
            LogDebug.WriteLine(DateTime.Now.ToString() + " [" + User + "]: " + fct + " " + mensaje);
            LogDebug.Close();
        }

        public static bool CaracteresProhibidos(string cadena)
        {
            char[] caract = {'*', '"', '·', '$', '%', '&', '/', '(', ')', '=', '?', '¿', 
							'ª', 'º', '|', '@', '#', '~', '€', '¬', '\'', '¡', '^', '*', '`',  
							'[', ']', '¨', '{', '}', '´', ';', ':', '.'};

            if (cadena.IndexOfAny(caract) > -1) return true;
            return false;
        }

        public static string FechaDesde(string fecha)
        {
            if (fecha == "")
                return "";
            else
            {
                try
                {
                    DateTime f_desde = DateTime.Parse(fecha);
                    return f_desde.ToString("dd/MM/yyyy");
                }
                catch
                {
                    DateTime iniEsteMes = DateTime.Today.AddDays(1 - DateTime.Today.Day); //Primer día de este mes
                    return iniEsteMes.ToString("dd/MM/yyyy");
                }
            }
        }

        public static string FechaHasta(string fecha)
        {
            if (fecha == "")
                return "";
            else
            {
                try
                {
                    DateTime f_hasta = DateTime.Parse(fecha);
                    return f_hasta.ToString("dd/MM/yyyy");
                }
                catch
                {
                    int numDiasParaFinal = DateTime.DaysInMonth(DateTime.Now.Year, DateTime.Now.Month) - DateTime.Now.Day;
                    DateTime finEsteMes = DateTime.Today.AddDays(numDiasParaFinal); //Ultimo día de este mes
                    return finEsteMes.ToString("dd/MM/yyyy");
                }
            }
        }

        public static string[] GenerarFiltros(DataSet ds, int NumColum, int longInicial)
        {
            string[] lista = null;
            if ((ds.Tables.Count == 0) ||
                (ds.Tables[0].Rows.Count == 0) ||
                (ds.Tables[0].Columns.Contains("Error")))
                return null;

            Redimensionar(ref lista);

            int i, cont;
            string filtro = "", aux;

            cont = 0;
            for (i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                aux = ds.Tables[0].Rows[i][NumColum].ToString();
                if (cont > 0)
                {
                    cont += 1;
                    filtro = filtro + "|";
                }
                cont = cont + aux.Length;
                if (cont < (LONGITUD_FILTROS - longInicial))
                    filtro = filtro + aux;
                else
                {
                    filtro = aux;
                    cont = aux.Length;
                    Redimensionar(ref lista);
                }
                lista[lista.Length - 1] = filtro;
            }
            return lista;
        }

        public static DataSet DataSetIgualesUnir(DataSet DsRes, DataSet dsParaAniadir)
        {
            int i;
            if (DsRes == null) return dsParaAniadir;
            if (DsRes.Tables.Count == 0) return dsParaAniadir;
            if (DsRes.Tables[0].Rows.Count == 0) return dsParaAniadir;
            if (DsRes.Tables[0].Columns.Contains("Error")) return dsParaAniadir;

            if (dsParaAniadir == null) return DsRes;
            if (dsParaAniadir.Tables.Count == 0) return DsRes;
            if (dsParaAniadir.Tables[0].Rows.Count == 0) return DsRes;
            if (dsParaAniadir.Tables[0].Columns.Contains("Error")) return DsRes;

            for (i = 0; i < dsParaAniadir.Tables[0].Rows.Count; i++)
                DsRes.Tables[0].ImportRow(dsParaAniadir.Tables[0].Rows[i]);

            DsRes.Tables[0].AcceptChanges();
            DsRes.AcceptChanges();
            return DsRes;
        }

        public static void Redimensionar(ref string[] Cadena)
        {
            if (Cadena == null)
            {
                string[] res = new string[1];
                Cadena = res;
                return;
            }
            else
            {
                string[] Caux = new string[Cadena.Length + 1];
                Cadena.CopyTo(Caux, 0);
                Cadena = Caux;
            }
        }

        public static bool ValidarControl(string stIni, string stCtrl)
        {
            int lenIni = 0;
            char[] cadIni;
            char[] cadFin;

            lenIni = stIni.Length / 2;

            cadIni = stIni.ToCharArray(0, lenIni);
            cadFin = stIni.ToCharArray(lenIni, stIni.Length - lenIni);

            char[] aux1 = new char[lenIni + cadFin.Length];
            char[] aux2 = new char[cadFin.Length];
            for (int i = 0; i < lenIni; i++)
            {
                aux1[i] = cadIni[lenIni - 1 - i];
            }

            for (int i = 0; i < aux2.Length; i++)
            {
                aux2[i] = cadFin[aux2.Length - 1 - i];
            }

            aux2.CopyTo(aux1, lenIni);

            char[] total = new char[aux1.Length];
            for (int i = 0; i < aux1.Length - 1; i += 2)
            {
                total[i] = aux1[i + 1];
                total[i + 1] = aux1[i];
            }

            if ((aux1.Length % 2) != 0)
                total[aux1.Length - 1] = aux1[aux1.Length - 1];

            stIni = new string(total);

            if (stIni == stCtrl)
                return true;

            return false;
        }

        public static string Contrasenia_Cryp_MD5(string texto)
        {
            byte[] resultadomd5;
            byte[] qsstrbytearray;
            int indice;

            System.Text.UnicodeEncoding codificacion = new System.Text.UnicodeEncoding();
            System.Security.Cryptography.MD5CryptoServiceProvider varmd5 = new System.Security.Cryptography.MD5CryptoServiceProvider();

            string codstr = "";
            string ret = "";

            if (texto == "")
                ret = "";
            else
            {
                qsstrbytearray = codificacion.GetBytes(texto);
                resultadomd5 = varmd5.ComputeHash(qsstrbytearray);

                for (indice = 0; indice < resultadomd5.Length; indice++)
                {
                    string tmp = resultadomd5[indice].ToString("X");
                    if (tmp.Length == 1)
                        codstr += "0";
                    codstr += tmp;
                }
                ret = codstr;
            }
            return ret;
        }


        public static NavisionDBUser Abrir_Login(string UserId, string Password, ref DataSet DsRes, NavisionDBConnection DBConn)
        {
            NavisionDBUser DBUser = null;
            DsRes = Utilidades.GenerarResultado("No conectado");
			DsRes.Tables[0].Columns.Add("Connected", Type.GetType("System.Boolean"), "false");
			DsRes.Tables[0].AcceptChanges();
			try
			{
                DBUser = DBConn.DoLogin(UserId, Password, ref DsRes);	
				
				//Obtengo los roles
				if ((DsRes.Tables.Count > 0) && (DsRes.Tables[0].Columns.Count > 0) && (!DsRes.Tables[0].Columns.Contains("Error")))
					if (Convert.ToBoolean(DsRes.Tables[0].Rows[0]["Connected"]) == true)
					{
						DsRes.Tables[0].Columns.Add("Administracion", System.Type.GetType("System.Boolean"));
						DsRes.Tables[0].Columns.Add("Gestion", System.Type.GetType("System.Boolean"));
						DsRes.Tables[0].Columns.Add("Compras", System.Type.GetType("System.Boolean"));
						DsRes.Tables[0].Columns.Add("Mensajería", System.Type.GetType("System.Boolean"));
						DsRes.Tables[0].Columns.Add("Comercial", System.Type.GetType("System.Boolean"));

                        NavisionDBTable dt = new NavisionDBTable(DBConn, DBUser);
                        NavisionDBCommand cmd = new NavisionDBCommand(DBConn);
						NavisionDBDataReader rd;

						dt.TableName = "Mobile Users";

						dt.AddColumn("Administracion");
						dt.AddColumn("Gestion");
						dt.AddColumn("Compras");
						dt.AddColumn("Mensajería");
						dt.AddColumn("Comercial");

						dt.AddFilter("User ID", UserId);
						dt.AddFilter("Password", Password);

						cmd.Table = dt;
						rd = cmd.ExecuteReader(false);

						if (rd.RecordsAffected > 0)
						{
							DsRes.Tables[0].Rows[0]["Administracion"] = rd.GetBoolean(0);
							DsRes.Tables[0].Rows[0]["Gestion"] = rd.GetBoolean(1);
							DsRes.Tables[0].Rows[0]["Compras"] = rd.GetBoolean(2);
							DsRes.Tables[0].Rows[0]["Mensajería"] = rd.GetBoolean(3);
							DsRes.Tables[0].Rows[0]["Comercial"] = rd.GetBoolean(4);
						}
							
						DsRes.Tables[0].AcceptChanges();
						DsRes.AcceptChanges();
					}

                return DBUser;
            }
            catch (Exception ex)
            {
                Utilidades.GenerarError(UserId, "Abrir_Login()", ex.Message);
                return null;
            }
        }

        public static bool CompruebaDNI(string nif_cif, string codpais, NavisionDB.NavisionDBConnection conn, NavisionDB.NavisionDBUser user)
        {
            NavisionDBTable dt = new NavisionDBTable(conn, user);
            NavisionDBCommand cmd = new NavisionDBCommand(conn);
            NavisionDBDataReader rd = new NavisionDBDataReader();


            nif_cif = nif_cif.Replace("-", "").Replace(".", "").Replace(",", "").Replace(":", "").Replace("_", "");

            // ------------------------------------------- \\
            // PRIMERO COMPROBAMOS QUE NO EXISTE DICHO DNI EN LA TABLA DE CLIENTES
            dt.TableName = "Customer";
            dt.AddColumn("No.");
            dt.AddColumn("VAT Registration No.");

            dt.AddFilter("VAT Registration No.", nif_cif);

            cmd.Table = dt;
            rd = cmd.ExecuteReader(false);

            return (rd.RecordsAffected != 0) ? false : CompruebaFormato(codpais, nif_cif, conn, user);
            //ya existe un cliente con ese dni
        }

        public static bool CompruebaFormato(string codpais, string dni, NavisionDB.NavisionDBConnection conn, NavisionDB.NavisionDBUser user)
        {
            NavisionDBTable dt = new NavisionDBTable(conn, user);
            //NavisionDBAdapter da = new NavisionDBAdapter();
            NavisionDBDataReader rd = new NavisionDBDataReader();
            NavisionDBCommand cmd = new NavisionDBCommand(conn);

            bool resul = false;

            //			if (codpais == "")
            //				codpais = "ES";

            // Accedemos a la tabla de formatos de dni por pais para contrastar
            dt.TableName = "VAT Registration No. Format";
            dt.AddColumn("Format");
            dt.AddFilter("Country Code", codpais);

            cmd.Table = dt;
            rd = cmd.ExecuteReader(false);
            if (rd.RecordsAffected != 0)
            {
                resul = CompruebaCadenas(rd.GetString(0), dni);
                while (!resul && rd.NextResult())
                {
                    resul = CompruebaCadenas(rd.GetString(0), dni);
                }
            }
            else
            {
                return true;
                // no existen formatos para ese pais, se aceptan todos
            }
            return resul;
        }

        public static bool CompruebaCadenas(string formato, string dni)
        {
            // cad[i] = # --> numero
            // cad[i] = @ --> letra

            dni = dni.ToUpper();
            if (formato.Length != dni.Length)
                return false;
            else
            {
                for (int i = 0; i < formato.Length; i++)
                {
                    switch (formato[i])
                    {
                        case '#':
                            if (!(dni[i] >= '0' && dni[i] <= '9'))
                                return false;
                            break;
                        case '@':
                            if (!(dni[i] >= 'A' && dni[i] <= 'Z'))
                                return false;
                            break;
                        default:
                            if (formato[i] != dni[i])
                                return false;
                            break;
                    }
                }
                return true;
            }
        }

        public static string CompruebaPais(string pais, NavisionDB.NavisionDBConnection conn, NavisionDB.NavisionDBUser user)
        {
            NavisionDBTable dt = new NavisionDBTable(conn, user);
            NavisionDBCommand cmd = new NavisionDBCommand(conn);
            NavisionDBDataReader rd = new NavisionDBDataReader();
            cmd = new NavisionDBCommand(conn);

            if (pais == "")
                return "";
            dt.TableName = "Country";
            dt.AddColumn("Code");
            dt.AddFilter("Code", pais);

            cmd.Table = dt;
            rd = cmd.ExecuteReader(false);

            return (rd.RecordsAffected != 0) ? rd.GetString(0) : "-1";
        }

        public static DataSet ListaPaises(NavisionDBConnection Conn)
        {
            NavisionDBUser user = new NavisionDBUser();
            user.UserId = "anonymous";

            NavisionDBTable dt = new NavisionDBTable(Conn, user);
            NavisionDBAdapter da = new NavisionDBAdapter();
            DataSet ds = new DataSet();

            dt.TableName = "Country";
            dt.AddColumn("Code");
            dt.AddColumn("Name");

            da.AddTable(dt);
            da.Fill(ref ds, false);
            return ds;
        }
        public static String  GenerarNombreFichero()
        {

            Random Nombre = new Random();

            return Nombre.Next(1,100000000).ToString()  ;
        }

        public static DataSet DameDatosTabla(string NombreTabla, string[] CamposConsultados, string[,] Filtros, string key,
            NavisionDBConnection Conn, NavisionDBUser User)
        {
            try
            {
                NavisionDBTable dt = new NavisionDBTable(Conn, User);
                NavisionDBAdapter da = new NavisionDBAdapter();
                DataSet ds = new DataSet();

                dt.TableName = NombreTabla;

                if (CamposConsultados != null)
                {
                    for (int i = 0; i < CamposConsultados.Length; i++)
                        dt.AddColumn(CamposConsultados[i]);
                }

                if (Filtros != null)
                {
                    for (int i = 0; i < Filtros.Length / Filtros.Rank; i++)
                        dt.AddFilter(Filtros[i, 0], Filtros[i, 1]);
                }

                if ((key != null) && (key != ""))
                    dt.KeyInNavisionFormat = key;

                da.AddTable(dt);
                da.Fill(ref ds, true);
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static DataSet VisorSucesos(DateTime Desde, DateTime Hasta, string Fuente)
        {
            try
            {
                System.Diagnostics.EventLog[] registrosEvent;
                // Gets logs on the local computer, gives remote computer name to get the logs on the remote computer.
                registrosEvent = System.Diagnostics.EventLog.GetEventLogs(System.Environment.MachineName);

                 
                DataSet ds = new DataSet("Visor de sucesos");
                ds.Tables.Add();
                ds.Tables[0].Columns.Add("Fecha");
                ds.Tables[0].Columns.Add("Tipo");
                ds.Tables[0].Columns.Add("Fuente");
                ds.Tables[0].Columns.Add("Mensaje");

                for (int i = 0; i < registrosEvent.Length; i++)
                {
                    if ((registrosEvent[i].Log == "Aplicación") || (registrosEvent[i].Log == "Application"))
                    {
                        if (registrosEvent[i].Entries.Count > 0)
                        {
                            int numEventos = 0;
                            for (int j = 0; j < registrosEvent[i].Entries.Count; j++)
                            {
                                if ((Desde.CompareTo(registrosEvent[i].Entries[j].TimeGenerated) <= 0) &&
                                    (Hasta.CompareTo(registrosEvent[i].Entries[j].TimeGenerated) >= 0) &&
                                    (Fuente == registrosEvent[i].Entries[j].Source) && (registrosEvent[i].Entries[j].EntryType.ToString() == "Advertencia"))
                                    numEventos += 1;
                            }
                            if (numEventos > 0)
                            {
                                int ind = 0;
                                for (int j = 0; j < registrosEvent[i].Entries.Count; j++)
                                {
                                    if ((Desde.CompareTo(registrosEvent[i].Entries[j].TimeGenerated) <= 0) &&
                                        (Hasta.CompareTo(registrosEvent[i].Entries[j].TimeGenerated) >= 0) &&
                                        (Fuente == registrosEvent[i].Entries[j].Source) && (registrosEvent[i].Entries[j].EntryType.ToString() == "Advertencia"))
                                    {
                                        ds.Tables[0].Rows.Add(ds.Tables[0].NewRow());

                                        ds.Tables[0].Rows[ind]["Fuente"] = registrosEvent[i].Entries[j].Source;
                                        ds.Tables[0].Rows[ind]["Tipo"] = registrosEvent[i].Entries[j].EntryType;
                                        if (registrosEvent[i].Entries[j].Message.Length > 80)
                                        {
                                            int Longitud = registrosEvent[i].Entries[j].Message.Length;
                                             
                                            ds.Tables[0].Rows[ind]["Mensaje"] =  registrosEvent[i].Entries[j].Message.Substring(Longitud -150 ,150) ;
                                        }
                                        else
                                        {
                                            ds.Tables[0].Rows[ind]["Mensaje"] = registrosEvent[i].Entries[j].Message;
                                        }
                                        ds.Tables[0].Rows[ind]["Fecha"] = registrosEvent[i].Entries[j].TimeGenerated;
                                        ind += 1;
                                    }
                                }
                                ds.Tables[0].AcceptChanges();
                            }
                        }
                        break;
                    }
                }
                string nombreFich = System.Configuration.ConfigurationManager.AppSettings["VISOR_SUCESOS"] + "\\" + DateTime.Now.ToString("dd MM yyyy hh_mm_ss") + ".evt";
                ds.WriteXml(nombreFich);
                return ds;
            }
            catch (Exception ex)
            {
                string str = ex.Message;
                //str = str;
                throw ex;
            }
        }
        public static DataSet EliminarRepetidos(ref DataSet ds, string ColumnName)
        {
            try
            {
                ds.Tables[0].DefaultView.Sort = ColumnName;
                if (ds.Tables[0].Rows.Count > 1)
                {
                    for (int i = 1; i < ds.Tables[0].Rows.Count; i++)
                    {
                        if (ds.Tables[0].DefaultView[i][ColumnName].ToString() ==
                            ds.Tables[0].DefaultView[i - 1][ColumnName].ToString())
                        {
                            ds.Tables[0].DefaultView[i].Delete();
                            ds.Tables[0].AcceptChanges();
                            i -= 1;
                        }
                    }
                }
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static string[] DividirCadenas(string Cadena, int tamaño)
        {
            int num_str = Cadena.Length / tamaño;
            int resto = (Cadena.Length % tamaño);
            int numTot = num_str;

            if (resto > 0) numTot++;

            string[] cads = new string[numTot];

            for (int i = 0; i < num_str; i++)
                cads[i] = Cadena.Substring(i * tamaño, tamaño);

            if (resto > 0) cads[numTot - 1] = Cadena.Substring(num_str * tamaño, Cadena.Length - (num_str * tamaño));

            return cads;
        }


        public static void CompletarDataSet(ref DataSet dsResult, bool stringEspacio, bool fechaBlanco)
        {
            for (int i = 0; i < dsResult.Tables.Count; i++)
            {
                for (int j = 0; j < dsResult.Tables[i].Rows.Count; j++)
                {
                    for (int k = 0; k < dsResult.Tables[i].Columns.Count; k++)
                    {
                        if (dsResult.Tables[i].Rows[j][k] == DBNull.Value)
                        {
                            System.TypeCode tipo = System.Type.GetTypeCode(dsResult.Tables[i].Columns[k].DataType);
                            switch (tipo)
                            {
                                case System.TypeCode.String:
                                    if (stringEspacio)
                                        dsResult.Tables[i].Rows[j][k] = " ";
                                    else
                                        dsResult.Tables[i].Rows[j][k] = string.Empty;
                                    break;
                                case System.TypeCode.DateTime:
                                    if (fechaBlanco == false)
                                        dsResult.Tables[i].Rows[j][k] = new DateTime(DateTime.MinValue.Ticks);

                                    break;
                                case System.TypeCode.Decimal:
                                case System.TypeCode.Int16:
                                case System.TypeCode.Int32:
                                case System.TypeCode.Int64:
                                    dsResult.Tables[i].Rows[j][k] = 0;
                                    break;

                                case System.TypeCode.Boolean:
                                    dsResult.Tables[i].Rows[j][k] = false;
                                    break;

                                default:
                                    break;
                            }
                        }
                    }
                }
                dsResult.Tables[i].AcceptChanges();
            }
            dsResult.AcceptChanges();
        } 

    }
}
