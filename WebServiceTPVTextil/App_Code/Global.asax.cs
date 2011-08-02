using System;
using System.Data;
using System.Collections;
using System.ComponentModel;
using System.Web;
using System.Web.SessionState;
using NavisionDB;
using MiddleWareTPVCentral;

namespace WebServiceTPVCentral
{
    /// <summary>
    /// Descripción breve de Global.
    /// </summary>
    public class Global : System.Web.HttpApplication
    {
        /// <summary>
        /// Variable del diseñador requerida.
        /// </summary>
        private System.ComponentModel.IContainer components = null;        
        public static NavisionDBConnection navConection;
        public static System.Data.SqlClient.SqlConnection sqlServer = null;

        public Global()
        {
            InitializeComponent();
        }

        protected void Application_Start(Object sender, EventArgs e)
        {

            //string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];

            //string Cx_Log = System.Configuration.ConfigurationManager.AppSettings["Cx_Log"];
            //string CxConfig = System.Configuration.ConfigurationManager.AppSettings["ConfFich"];

            //System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
            //LogDebug.WriteLine(DateTime.Now.ToString() + " ######### Iniciando el servicio ..........");
            //LogDebug.Close();

            try
            {
                navConection = MiddleWareTPVCentral.Utilidades.Conectar_Navision();

                string nombre_servidor_colas = System.Configuration.ConfigurationManager.AppSettings["NOMBRE_SERVIDOR_COLAS"];
                string colaToNav = System.Configuration.ConfigurationManager.AppSettings["COLA_TO_NAVISION"];
                string colaFromNav = System.Configuration.ConfigurationManager.AppSettings["COLA_FROM_NAVISION"];
                
                NavisionDBNas nas = new NavisionDBNas();
                try
                {

                    nas.IniciarNas(nombre_servidor_colas, colaFromNav, colaToNav);
                }
                catch (Exception ex)
                {
                    string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];
                    System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
                    LogDebug = new System.IO.StreamWriter(Error_Log, true);
                    LogDebug.WriteLine(DateTime.Now.ToString() + " ERROR al iniciar el Nas: " + ex.Message);
                    LogDebug.Close();

                    string MailMsg = DateTime.Now.ToString() + " ERROR al iniciar el Nas: " + ex.Message;
                    string MailSubject = "WSTPV Error: NAS";
                    MiddleWareTPVCentral.Utilidades.SendMail(MailMsg, MailSubject);
                }
            }
            catch (Exception ex)
            {
                string Error_Log = System.Configuration.ConfigurationManager.AppSettings["Error_Log"];
                System.IO.StreamWriter LogDebug = new System.IO.StreamWriter(Error_Log, true);
                LogDebug = new System.IO.StreamWriter(Error_Log, true);
                LogDebug.WriteLine(DateTime.Now.ToString() + " ERROR en conexión: " + ex.Message);
                LogDebug.Close();

                string MailMsg = DateTime.Now.ToString() + " ERROR de conexión: " + ex.Message;
                string MailSubject = "WSTPV Error: Conexión";
                MiddleWareTPVCentral.Utilidades.SendMail(MailMsg, MailSubject);

                throw new Exception(ex.Message);
            }
        }

        protected void Session_Start(Object sender, EventArgs e)
        {

            
        }

        protected void Application_BeginRequest(Object sender, EventArgs e)
        {

        }

        protected void Application_EndRequest(Object sender, EventArgs e)
        {

        }

        protected void Application_AuthenticateRequest(Object sender, EventArgs e)
        {

        }

        protected void Application_Error(Object sender, EventArgs e)
        {

        }

        protected void Session_End(Object sender, EventArgs e)
        {
            Session["DBUser"] = null;
        }

        protected void Application_End(Object sender, EventArgs e)
        {
            
        }

        #region Código generado por el Diseñador de Web Forms
        /// <summary>
        /// Método necesario para admitir el Diseñador. No se puede modificar
        /// el contenido del método con el editor de código.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
        }
        #endregion
    }
}

