namespace NavisionDB
{
    using Microsoft.Navision.CFront;
    using System;
    using System.ComponentModel;
    using System.Data;
    using System.Data.SqlClient;
    using System.IO;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;

    public class NavisionDBConnection : Component
    {
        private string archivoLog;
        private string basedatos;
        private int cache;
        private string companyia;
        private bool conectado;
        private string contrasenya;
        private ManualResetEvent evento = new ManualResetEvent(false);
        //private const string k = "zUnycDg6npT8OmgxCWZmPg==";
        public static int[] nLT = new int[1000];
        private static int NumeroSesiones = 0;
        private string rutaDeAplicacion;
        private string servidor;
        private NavisionNetType tipoRed;
        private string usuario;
        internal static bool varEditMode = false;

        internal void AllowAll()
        {
            CFrontDll.AllowAll();
        }

        public void AWT()
        {
            CFrontDll.AWT();
        }

        public void BWT()
        {
            CFrontDll.BWT();
        }

        private string c(string clave)
        {
            MD5CryptoServiceProvider provider = new MD5CryptoServiceProvider();
            byte[] bytes = Encoding.UTF8.GetBytes(clave);
            byte[] inArray = provider.ComputeHash(bytes);
            provider.Clear();
            return Convert.ToBase64String(inArray);
        }

        public void close()
        {
            try
            {
                CFrontDll.CloseCompany();
                CFrontDll.DisconnectServer();
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message);
            }
        }

        private void CloseCompany()
        {
            CFrontDll.CloseCompany();
        }

        private void CloseDatabase()
        {
            this.CloseCompany();
            CFrontDll.CloseDatabase();
        }

        internal void ConnectServerandOpenDataBase(string ServerName, NavisionNetType NetType, string DatabaseName, int CacheSize, bool UseCommitCache, bool UseNTAuthentication, string UserID, string Password)
        {
            CFrontDll.ConnectServerandOpenDataBase(ServerName, NetType, DatabaseName, CacheSize, UseCommitCache, UseNTAuthentication, UserID, Password);
        }

        private string CryptPassword(string userId, string password)
        {
            CFrontDll.CryptPassword(userId, password);
            return password.ToString();
        }

        private void DisconnectServer()
        {
            this.CloseCompany();
            CFrontDll.DisconnectServer();
        }

        public NavisionDBUser DoLogin(string User, string Password, ref DataSet Ds)
        {
            NavisionDBUser user2;
            try
            {
                NavisionDBUser user = new NavisionDBUser();
                user.UserId = User;
                user.Password = Password;
                user.ValidateUser();
                Ds = user.Dataset;                
                user2 = user;
            }
            catch (Exception exception)
            {                   
                StreamWriter writer = new StreamWriter(this.archivoLog,true);
                writer.WriteLine(DateTime.Now.ToString() + "["+ User +"]: Abrir_Login() " + exception.Message);
                writer.Close();
                throw exception;
            }
            return user2;
        }

        public void DoLogout(NavisionDBUser NavisionUser)
        {
            if (NumeroSesiones > 0)
            {
                NumeroSesiones--;
            }
            NavisionUser.DisposeUser(NavisionUser.Email, NavisionUser.Password, NavisionUser.UserKey);
        }

        public void EWT()
        {
            CFrontDll.EWT();
        }

        internal void InitCfront(NavisionDriverType driver, string path)
        {
            CFrontDll.CFrontIni(driver, path);
        }

        private string NextCompany(string company)
        {
            return CFrontDll.NextCompany(company);
        }

        public void open()
        {
            try
            {
                if (this.archivoLog == "")
                {
                    throw new Exception("Es necesario especificar un fichero de log");
                }
                StreamWriter writer = new StreamWriter(this.archivoLog);
                writer.WriteLine(DateTime.Now.ToString() + " Creando proceso de conexión");
                writer.Close();
                this.STAopen();
                if (!this.conectado)
                {
                    throw new Exception("Un error ha ocurrido al intentar conectar a Navision");
                }
                writer = new StreamWriter(this.archivoLog, true);
                writer.WriteLine(DateTime.Now.ToString() + " Comprobando licencia...");
                writer.Close();

                /*
                if (!this.vLN("zUnycDg6npT8OmgxCWZmPg=="))
                {
                    this.close();
                    throw new Exception("License number INCORRECT");
                }
                */

                writer = new StreamWriter(this.archivoLog, true);
                writer.WriteLine(DateTime.Now.ToString() + " Número de licencia correcto...");
                writer.WriteLine(DateTime.Now.ToString() + " CONECTADO");
                writer.Close();
            }
            catch (Exception exception)
            {
                StreamWriter writer2 = new StreamWriter(this.archivoLog, true);
                writer2.WriteLine(DateTime.Now.ToString() + " ERROR: " + exception.Message);
                writer2.Close();
                throw new Exception(exception.Message);
            }
        }

        private void OpenCompany(int IdNo)
        {
            string company = null;
            int num = 0;
            do
            {
                company = CFrontDll.NextCompany(company);
                if (num == IdNo)
                {
                    CFrontDll.OpenCompany(company);
                }
                num++;
            }
            while ((num < IdNo) && (company.Length > 0));
        }

        private void OpenCompany(string company)
        {
            CFrontDll.OpenCompany(company);
        }

        internal CFrontRecordset OpenTable(int tableNo)
        {
            int num;
            CFrontDll.OpenTable(out num, tableNo);
            return new CFrontRecordset(num);
        }

        internal CFrontRecordset OpenTable(string tableName)
        {
            int tableNo = this.TableNo(tableName);
            return this.OpenTable(tableNo);
        }

        private CFrontRecordset OpenTemporaryTable(int tableNo)
        {
            int num;
            CFrontDll.OpenTable(out num, tableNo);
            return new CFrontRecordset(num);
        }

        private CFrontRecordset OpenTemporaryTable(string tableName)
        {
            int tableNo = this.TableNo(tableName);
            return this.OpenTemporaryTable(tableNo);
        }

        internal void SelectLatestVersion()
        {
            CFrontDll.SelectLatestVersion();
        }

        private void STAopen()
        {
            try
            {
                StreamWriter writer = new StreamWriter(this.archivoLog, true);
                writer.WriteLine(DateTime.Now.ToString() + " Estableciendo ruta de Navision");
                writer.Close();
                this.InitCfront(NavisionDriverType.Sql, this.rutaDeAplicacion);
                writer = new StreamWriter(this.archivoLog, true);
                writer.WriteLine(DateTime.Now.ToString() + " Obteniendo última version");
                writer.Close();
                this.SelectLatestVersion();
                SqlConnection connection = null;
                string connectionString = "Data Source=" + this.server + ";Initial Catalog=" + this.basedatos + "; user=" + this.user + "; password=" + this.password + ";";
                if (connection == null)
                {
                    connection = new SqlConnection(connectionString);
                }
                try
                {
                    if (connection.State != ConnectionState.Open)
                    {
                        connection.Open();
                    }
                }
                catch (Exception exception)
                {
                    throw new Exception(exception.Message);
                }
                new SqlCommand("UPDATE [" + this.basedatos + "].dbo.[$ndo$dbproperty] SET [databaseversionno] =60", connection).ExecuteNonQuery();
                writer = new StreamWriter(this.archivoLog, true);
                writer.WriteLine(string.Concat(new object[] { DateTime.Now.ToString(), " Conectando al servidor ", this.servidor, ":", this.tipoRed }));
                writer.Close();
                try
                {
                    this.ConnectServerandOpenDataBase(this.server, this.tipoRed, this.basedatos, this.cache, true, false, this.user, this.password);
                    writer = new StreamWriter(this.archivoLog, true);
                    writer.WriteLine(string.Concat(new object[] { DateTime.Now.ToString(), " Conectando al servidor ", this.servidor, ":", this.tipoRed, " OK" }));
                    writer.Close();
                }
                catch
                {
                    writer = new StreamWriter(this.archivoLog, true);
                    writer.WriteLine(string.Concat(new object[] { DateTime.Now.ToString(), " Fallo al conectar servidor ", this.servidor, ":", this.tipoRed, " OK" }));
                    writer.Close();
                    new SqlCommand("UPDATE [" + this.basedatos + "].dbo.[$ndo$dbproperty] SET [databaseversionno] =80", connection).ExecuteNonQuery();
                    connection.Dispose();
                }
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                new SqlCommand("UPDATE [" + this.basedatos + "].dbo.[$ndo$dbproperty] SET [databaseversionno] =80", connection).ExecuteNonQuery();
                connection.Dispose();
                writer = new StreamWriter(this.archivoLog, true);
                writer.WriteLine(DateTime.Now.ToString() + " Abriendo empresa :" + this.company);
                writer.Close();
                this.AllowAll();
                this.OpenCompany(this.company);
                this.conectado = true;
            }
            catch (Exception exception2)
            {
                StreamWriter writer2 = new StreamWriter(this.archivoLog, true);
                writer2.WriteLine(DateTime.Now.ToString() + " ERROR: - " + exception2.Message);
                writer2.Close();
                throw new Exception(exception2.Message);
            }
        }

        private string TableName(int hTable)
        {
            return CFrontDll.TableName(hTable);
        }

        internal int TableNo(string tableName)
        {
            return CFrontDll.TableNo(tableName);
        }

        private string UserID()
        {
            return CFrontDll.UserID();
        }

        private bool vLN(string valor)
        {
            this.SelectLatestVersion();
            CFrontRecordset recordset = this.OpenTable(0x77359428);
            int[] fieldNumbers = recordset.FieldNumbers;
            recordset.MoveFirst();
            if (!recordset.EOF)
            {
                do
                {
                    string clave = Convert.ToString(recordset[fieldNumbers[1]]);
                    if (this.c(clave) == valor)
                    {
                        return true;
                    }
                }
                while ((recordset.MoveStep(1) == 1) && (Convert.ToInt32(recordset[fieldNumbers[0]]) <= 4));
            }
            return false;
        }

        public string applicationPath
        {
            get
            {
                return this.rutaDeAplicacion;
            }
            set
            {
                this.rutaDeAplicacion = value;
            }
        }

        public int cachesize
        {
            get
            {
                return this.cache;
            }
            set
            {
                this.cache = value;
            }
        }

        public string company
        {
            get
            {
                return this.companyia;
            }
            set
            {
                this.companyia = value;
            }
        }

        public string DBname
        {
            get
            {
                return this.basedatos;
            }
            set
            {
                this.basedatos = value;
            }
        }

        internal static bool EditMode
        {
            get
            {
                return varEditMode;
            }
            set
            {
                varEditMode = value;
            }
        }

        public string logFile
        {
            get
            {
                return this.archivoLog;
            }
            set
            {
                this.archivoLog = value;
            }
        }

        public string netType
        {
            get
            {
                if (this.tipoRed == NavisionNetType.NativeTcp)
                {
                    return "TCP";
                }
                if (this.tipoRed == NavisionNetType.SqlDefault)
                {
                    return "SQLTCP";
                }
                return "DEFAULT";
            }
            set
            {
                if (value == "TCP")
                {
                    this.tipoRed = NavisionNetType.NativeTcp;
                }
                if (value == "SQLTCP")
                {
                    this.tipoRed = NavisionNetType.SqlDefault;
                }
            }
        }

        public string password
        {
            get
            {
                return this.contrasenya;
            }
            set
            {
                this.contrasenya = value;
            }
        }

        public string server
        {
            get
            {
                return this.servidor;
            }
            set
            {
                this.servidor = value;
            }
        }

        public string user
        {
            get
            {
                return this.usuario;
            }
            set
            {
                this.usuario = value;
            }
        }
    }
}

