namespace NavisionDB
{
    using System;
    using System.Data;
    using System.Messaging;
    using System.Runtime.CompilerServices;
    using System.Threading;

    public class NavisionDBNas
    {
        private bool ColasInicializadas = false;
        private string[,] controlParams;
        private DataSet ds = new DataSet();
        private const string ENVIADO = "Enviado";
        private const int ESPERA_CON_MENSAJES = 0x1388;
        private string Funcion;
        private MessageQueue msqfromnavision;
        public string MsqServerName;
        private MessageQueue msqtonavision;
        private string[] Parameter;
        private const int POS_ESTADO = 1;
        private const int POS_LABEL = 0;
        private const string RECIBIDO = "Recibido";

        private bool BuscaIDMensajes(string ID)
        {
            for (int i = 0; i < this.Parameter.Length; i++)
            {
                if (this.controlParams[i, 0] == ID)
                {
                    this.controlParams[i, 1] = "Recibido";
                    return true;
                }
            }
            return false;
        }

        public void CerrarNas()
        {
            MessageQueue.Delete(this.msqfromnavision.Path);
            MessageQueue.Delete(this.msqtonavision.Path);
        }

        public void IniciarNas(string MsqServerName, string msqsqlfromnavision, string msqsqltonavision)
        {
            if (!MessageQueue.Exists(MsqServerName + @"\" + msqsqlfromnavision))
            {
                MessageQueue.Create(MsqServerName + @"\" + msqsqlfromnavision, false);
            }
            if (!MessageQueue.Exists(MsqServerName + @"\" + msqsqltonavision))
            {
                MessageQueue.Create(MsqServerName + @"\" + msqsqltonavision, false);
            }
            MessageQueue queue = new MessageQueue();
            queue.Path = MsqServerName + @"\" + msqsqlfromnavision;
            queue.Purge();
            queue.Path = MsqServerName + @"\" + msqsqltonavision;
            queue.Purge();
        }

        private void InitializeComponent(string MsqServerName, string msqsqlfromnavision, string msqsqltonavision)
        {
            this.msqfromnavision = new MessageQueue();
            this.msqtonavision = new MessageQueue();
            if (!MessageQueue.Exists(MsqServerName + @"\" + msqsqlfromnavision))
            {
                throw new Exception("MSMQ no existe");
            }
            this.msqfromnavision.Path = MsqServerName + @"\" + msqsqlfromnavision;
            if (!MessageQueue.Exists(MsqServerName + @"\" + msqsqltonavision))
            {
                throw new Exception("MSMQ no existe");
            }
            this.msqtonavision.Path = MsqServerName + @"\" + msqsqltonavision;
        }

        public void NasInitializeChannels(string MsqServerName, string msqsqlfromnavision, string msqsqltonavision)
        {
            this.InitializeComponent(MsqServerName, msqsqlfromnavision, msqsqltonavision);
            this.msqfromnavision.Formatter = new XmlMessageFormatter(new Type[] { typeof(DataSet) });
            this.ColasInicializadas = true;
        }

        private bool RecibidosTodos()
        {
            for (int i = 0; i < this.Parameter.Length; i++)
            {
                if (this.controlParams[i, 1] == "Enviado")
                {
                    return false;
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SendParams(NavisionDBNas nas, int MiliSegEspera)
        {
            bool flag = false;
            nas.controlParams = new string[nas.Parameter.Length, 2];
            string str2 = "";
            if (!nas.ColasInicializadas)
            {
                throw new Exception("Debe inicializarle los canales de comunicaci\x00f3n");
            }
            MessageQueue queue = new MessageQueue();
            queue.Path = nas.msqfromnavision.Path;
            queue.Purge();
            str2 = GeneradorIDMsg.NewMsgId();
            string str = nas.Funcion + "(";
            for (int i = 0; i < nas.Parameter.Length; i++)
            {
                str = str + nas.Parameter[i] + ",---,";
            }
            str = str + str2 + ")";
            nas.msqtonavision.Send(str, "Navision MSMQ-BA");
            flag = false;
            Thread.Sleep(150);
            nas.msqfromnavision.Peek(new TimeSpan(0, 0, 0, 0, MiliSegEspera));
            for (int j = 0; j < 2; j++)
            {
                if (j != 0)
                {
                    nas.msqfromnavision.Peek(new TimeSpan(0, 0, 0, 0, MiliSegEspera / 2));
                }
                foreach (Message message in nas.msqfromnavision.GetAllMessages())
                {
                    nas.msqfromnavision.Receive(new TimeSpan(0, 0, 0, 0, 0x1388));
                    nas.ds = new DataSet();
                    nas.ds.ReadXml(message.BodyStream, XmlReadMode.Auto);
                    if (((nas.ds.Tables.Count > 0) && (nas.ds.Tables[0].Rows.Count > 0)) && (nas.ds.Tables[0].Rows[0][0].ToString() == str2))
                    {
                        flag = true;
                        break;
                    }
                }
                if (flag)
                {
                    break;
                }
            }
            if (!flag)
            {
                throw new Exception("Error de TIME_OUT en la ejecuci\x00f3n de " + nas.Funcion + ".");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SendParams(NavisionDBNas nas, int MiliSegEspera, string Identificador, bool UnSoloEnvio)
        {
            string str;
            bool flag = false;
            nas.controlParams = new string[nas.Parameter.Length, 2];
            string str2 = "";
            if (!nas.ColasInicializadas)
            {
                throw new Exception("Debe inicializarle los canales de comunicaci\x00f3n");
            }
            MessageQueue queue = new MessageQueue();
            queue.Path = nas.msqfromnavision.Path;
            queue.Purge();
            if (UnSoloEnvio)
            {
                for (int i = 0; i < nas.Parameter.Length; i++)
                {
                    try
                    {
                        str2 = GeneradorIDMsg.NewMsgId();
                        str = nas.Funcion + "(" + Identificador + ",---," + nas.Parameter[i] + ",---," + str2 + ")";
                        nas.msqtonavision.Send(str, "Navision MSMQ-BA");
                        flag = false;
                        Thread.Sleep(150);
                        nas.msqfromnavision.Peek(new TimeSpan(0, 0, 0, 0, MiliSegEspera));
                        for (int j = 0; j < 2; j++)
                        {
                            if (j != 0)
                            {
                                nas.msqfromnavision.Peek(new TimeSpan(0, 0, 0, 0, MiliSegEspera / 2));
                            }
                            foreach (Message message in nas.msqfromnavision.GetAllMessages())
                            {
                                nas.msqfromnavision.Receive(new TimeSpan(0, 0, 0, 0, 0x1388));
                                nas.ds = new DataSet();
                                nas.ds.ReadXml(message.BodyStream, XmlReadMode.Auto);
                                if (((nas.ds.Tables.Count > 0) && (nas.ds.Tables[0].Rows.Count > 0)) && (nas.ds.Tables[0].Rows[0][0].ToString() == str2))
                                {
                                    flag = true;
                                    break;
                                }
                            }
                            if (flag)
                            {
                                break;
                            }
                        }
                        if (!flag)
                        {
                            throw new Exception("Error en la ejecuci\x00f3n de " + nas.Funcion + ".");
                        }
                    }
                    catch (MessageQueueException exception)
                    {
                        throw new Exception("Se ha producido un error en la ejecuci\x00f3n de " + nas.Funcion + " " + exception.Message);
                    }
                }
            }
            else
            {
                str2 = GeneradorIDMsg.NewMsgId();
                str = nas.Funcion + "(";
                for (int k = 0; k < nas.Parameter.Length; k++)
                {
                    str = str + nas.Parameter[k] + ",\x00b7\x00b7\x00b7,";
                }
                str = str + str2 + ")";
                try
                {
                    nas.msqtonavision.Send(str, "Navision MSMQ-BA");
                    flag = false;
                    Thread.Sleep(150);
                    nas.msqfromnavision.Peek(new TimeSpan(0, 0, 0, 0, MiliSegEspera));
                    for (int m = 0; m < 2; m++)
                    {
                        if (m != 0)
                        {
                            nas.msqfromnavision.Peek(new TimeSpan(0, 0, 0, 0, MiliSegEspera / 2));
                        }
                        foreach (Message message2 in nas.msqfromnavision.GetAllMessages())
                        {
                            nas.msqfromnavision.Receive(new TimeSpan(0, 0, 0, 0, 0x1388));
                            nas.ds = new DataSet();
                            nas.ds.ReadXml(message2.BodyStream, XmlReadMode.Auto);
                            if (((nas.ds.Tables.Count > 0) && (nas.ds.Tables[0].Rows.Count > 0)) && (nas.ds.Tables[0].Rows[0][0].ToString() == str2))
                            {
                                flag = true;
                                break;
                            }
                        }
                        if (flag)
                        {
                            break;
                        }
                    }
                    if (!flag)
                    {
                        throw new Exception("Error de TIME_OUT en la ejecuci\x00f3n de " + nas.Funcion + ".");
                    }
                }
                catch (MessageQueueException exception2)
                {
                    throw new Exception("1. Se ha producido un error en la ejecuci\x00f3n de " + nas.Funcion + "(): ->  " + exception2.Message);
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SendParams_old(NavisionDBNas nas, int MiliSegEspera, string Identificador, bool UnSoloEnvio)
        {
            string str;
            nas.controlParams = new string[nas.Parameter.Length, 2];
            string str2 = "";
            if (!nas.ColasInicializadas)
            {
                throw new Exception("Debe inicializarle los canales de comunicaci\x00f3n");
            }
            if (UnSoloEnvio)
            {
                for (int i = 0; i < nas.Parameter.Length; i++)
                {
                    try
                    {
                        str2 = GeneradorIDMsg.NewMsgId();
                        str = nas.Funcion + "(" + Identificador + ",---," + nas.Parameter[i] + ",---," + str2 + ")";
                        nas.msqtonavision.Send(str, "Navision MSMQ-BA");
                        nas.controlParams[i, 0] = str2;
                        nas.controlParams[i, 1] = "Enviado";
                    }
                    catch (MessageQueueException exception)
                    {
                        throw new Exception("Ha habido un error en la ejecuci\x00f3n de " + nas.Funcion + " " + exception.Message);
                    }
                }
            }
            else
            {
                str2 = GeneradorIDMsg.NewMsgId();
                str = nas.Funcion + "(";
                for (int j = 0; j < nas.Parameter.Length; j++)
                {
                    str = str + nas.Parameter[j] + ",---,";
                }
                str = str + str2 + ")";
                try
                {
                    nas.msqtonavision.Send(str, "Navision MSMQ-BA");
                }
                catch (MessageQueueException exception2)
                {
                    throw new Exception("Ha habido un error en la ejecuci\x00f3n de " + nas.Funcion + " " + exception2.Message);
                }
            }
            Thread.Sleep(MiliSegEspera);
            try
            {
                new Message();
                nas.msqfromnavision.Peek(new TimeSpan(0, 0, 0, 10));
                for (int k = 0; k < 2; k++)
                {
                    if (k != 0)
                    {
                        Thread.Sleep(0x1388);
                    }
                    foreach (Message message in nas.msqfromnavision.GetAllMessages())
                    {
                        nas.ds = new DataSet();
                        nas.ds.ReadXml(message.BodyStream, XmlReadMode.Auto);
                        if ((nas.ds.Tables.Count > 0) && (nas.ds.Tables[0].Rows.Count > 0))
                        {
                            if (UnSoloEnvio)
                            {
                                if (nas.BuscaIDMensajes(nas.ds.Tables[0].Rows[0][0].ToString()))
                                {
                                    nas.msqfromnavision.ReceiveById(message.Id, new TimeSpan(0, 0, 0, 10));
                                }
                            }
                            else if (((nas.ds.Tables.Count > 0) && (nas.ds.Tables[0].Rows.Count > 0)) && (nas.ds.Tables[0].Rows[0][0].ToString() == str2))
                            {
                                nas.msqfromnavision.ReceiveById(message.Id, new TimeSpan(0, 0, 0, 10));
                                return;
                            }
                        }
                    }
                    if (!UnSoloEnvio)
                    {
                        throw new Exception("Ha habido un error en la ejecuci\x00f3n de " + nas.Funcion + " TIME OUT");
                    }
                    if (nas.RecibidosTodos())
                    {
                        return;
                    }
                }
                throw new Exception("Ha habido un error en la ejecuci\x00f3n de " + nas.Funcion + " TIME OUT");
            }
            catch (MessageQueueException exception3)
            {
                if (exception3.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                {
                    throw new Exception("Ha habido un error en la ejecuci\x00f3n de " + nas.Funcion + " " + exception3.Message);
                }
                throw exception3;
            }
            catch (Exception exception4)
            {
                throw exception4;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SendParamsAsync(NavisionDBNas nas)
        {
            nas.controlParams = new string[nas.Parameter.Length, 2];
            if (!nas.ColasInicializadas)
            {
                throw new Exception("Debe inicializarle los canales de comunicaci\x00f3n");
            }
            MessageQueue queue = new MessageQueue();
            queue.Path = nas.msqfromnavision.Path;
            string str = nas.Funcion + "(";
            for (int i = 0; i < nas.Parameter.Length; i++)
            {
                str = str + nas.Parameter[i] + ",---,";
            }
            str = str + ")";
            nas.msqtonavision.Send(str, "Navision MSMQ-BA");
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void SendParamsAsync(NavisionDBNas nas, string Identificador, bool UnSoloEnvio)
        {
            string str;
            nas.controlParams = new string[nas.Parameter.Length, 2];
            string str2 = "";
            if (!nas.ColasInicializadas)
            {
                throw new Exception("Debe inicializarle los canales de comunicaci\x00f3n");
            }
            MessageQueue queue = new MessageQueue();
            queue.Path = nas.msqfromnavision.Path;
            if (UnSoloEnvio)
            {
                for (int i = 0; i < nas.Parameter.Length; i++)
                {
                    try
                    {
                        str2 = GeneradorIDMsg.NewMsgId();
                        str = nas.Funcion + "(" + Identificador + ",---," + nas.Parameter[i] + ",---," + str2 + ")";
                        nas.msqtonavision.Send(str, "Navision MSMQ-BA");
                    }
                    catch (MessageQueueException exception)
                    {
                        throw new Exception("Se ha producido un error en la ejecuci\x00f3n de " + nas.Funcion + " " + exception.Message);
                    }
                }
            }
            else
            {
                str2 = GeneradorIDMsg.NewMsgId();
                //string strAux = null;
                str = nas.Funcion + "(";
                for (int j = 0; j < nas.Parameter.Length; j++)
                {
                    /*
                    if (String.IsNullOrEmpty(strAux))
                    {
                        strAux = nas.Parameter[j];
                    }
                    else
                    {
                        strAux += ",---," + nas.Parameter[j];
                    }
                    */
                    str = str + nas.Parameter[j] + ",---,";
                }
                str = str + str2 + ")";
                //str += strAux + ")";
 
                try
                {
                    nas.msqtonavision.Send(str, "Navision MSMQ-BA");
                }
                catch (MessageQueueException exception2)
                {
                    throw new Exception("1. Se ha producido un error en la ejecuci\x00f3n de " + nas.Funcion + "(): ->  " + exception2.Message);
                }
            }
        }

        public DataSet Answer
        {
            get
            {
                if (!this.ColasInicializadas)
                {
                    throw new Exception("Debe inicializarle los canales de comunicaci\x00f3n");
                }
                return this.ds;
            }
        }

        public string ExecuteFunction
        {
            set
            {
                this.Funcion = value;
            }
        }

        public string[] Parameters
        {
            set
            {
                this.Parameter = value;
            }
        }
    }
}

