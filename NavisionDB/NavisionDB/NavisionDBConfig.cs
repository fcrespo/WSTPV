namespace NavisionDB
{
    using System;
    using System.IO;
    using System.Xml.Serialization;

    public class NavisionDBConfig
    {
        private bool appSettingsChanged = false;
        private string basedatos = "";
        private int cache = 0;
        private string compañia = "";
        private string contraseña = "webmobile";
        private string ruta = "";
        private string servidor = "";
        private string TipoRed = "";
        private string usuario = "webmobile";

        public bool LoadAppSettings(string pathFich)
        {
            XmlSerializer serializer = null;
            FileStream stream = null;
            bool flag = false;
            try
            {
                serializer = new XmlSerializer(typeof(NavisionDBConfig));
                FileInfo info = new FileInfo(pathFich);
                if (info.Exists)
                {
                    stream = info.OpenRead();
                    NavisionDBConfig config = (NavisionDBConfig) serializer.Deserialize(stream);
                    this.servidor = config.servidor;
                    this.TipoRed = config.TipoRed;
                    this.usuario = config.usuario;
                    this.contraseña = config.contraseña;
                    this.compañia = config.compañia;
                    this.ruta = config.ruta;
                    this.DBName = config.DBName;
                    this.cachesize = config.cachesize;
                    flag = true;
                }
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message);
            }
            finally
            {
                if (stream != null)
                {
                    stream.Close();
                }
            }
            if (this.TipoRed == "")
            {
                this.TipoRed = "tcp";
                this.appSettingsChanged = true;
            }
            return flag;
        }

        public bool SaveAppSettings(string pathFich)
        {
            if (this.appSettingsChanged)
            {
                StreamWriter writer = null;
                XmlSerializer serializer = null;
                try
                {
                    serializer = new XmlSerializer(typeof(NavisionDBConfig));
                    writer = new StreamWriter(pathFich, false);
                    serializer.Serialize((TextWriter) writer, this);
                }
                catch (Exception exception)
                {
                    throw new Exception(exception.Message);
                }
                finally
                {
                    if (writer != null)
                    {
                        writer.Close();
                    }
                }
            }
            return this.appSettingsChanged;
        }

        public string ApplicationPath
        {
            get
            {
                return this.ruta;
            }
            set
            {
                if (value != this.ruta)
                {
                    this.ruta = value;
                    this.appSettingsChanged = true;
                }
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
                if (value != this.cache)
                {
                    this.cache = value;
                    this.appSettingsChanged = true;
                }
            }
        }

        public string Company
        {
            get
            {
                return this.compañia;
            }
            set
            {
                if (value != this.compañia)
                {
                    this.compañia = value;
                    this.appSettingsChanged = true;
                }
            }
        }

        public string DBName
        {
            get
            {
                return this.basedatos;
            }
            set
            {
                if (value != this.basedatos)
                {
                    this.basedatos = value;
                    this.appSettingsChanged = true;
                }
            }
        }

        public string NetType
        {
            get
            {
                return this.TipoRed;
            }
            set
            {
                if (value != this.TipoRed)
                {
                    this.TipoRed = value;
                    this.appSettingsChanged = true;
                }
            }
        }

        public string Password
        {
            get
            {
                return this.contraseña;
            }
            set
            {
                if (value != this.contraseña)
                {
                    this.contraseña = value;
                    this.appSettingsChanged = true;
                }
            }
        }

        public string Server
        {
            get
            {
                return this.servidor;
            }
            set
            {
                if (value != this.servidor)
                {
                    this.servidor = value;
                    this.appSettingsChanged = true;
                }
            }
        }

        public string User
        {
            get
            {
                return this.usuario;
            }
            set
            {
                if (value != this.usuario)
                {
                    this.usuario = value;
                    this.appSettingsChanged = true;
                }
            }
        }
    }
}

