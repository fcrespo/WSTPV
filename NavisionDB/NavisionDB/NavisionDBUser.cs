namespace NavisionDB
{
    using System;
    using System.Data;

    [Serializable]
    public class NavisionDBUser
    {
        private int Clave = -1;
        private bool Conectado = false;
        private string Contraseña = "";
        private string CorreoE = "";
        private DateTime FechaHoraConexion = new DateTime();
        private string NivelDeRestriccion = "";
        private string Nombre_usuario = "";
        private int numConexiones = 0;
        private string Tipo_Usuario = "";
        private string UsuarioCode = "";
        private string UsuarioId = "";

        private void AWT()
        {
            CFrontDll.AWT();
        }

        private void BWT()
        {
            CFrontDll.BWT();
        }

        public void DisposeUser(string User, string Password, int UserKey)
        {
        }

        private void EWT()
        {
            CFrontDll.EWT();
        }

        private CFrontRecordset OpenTable(int tableNo)
        {
            int num;
            CFrontDll.OpenTable(out num, tableNo);
            return new CFrontRecordset(num);
        }

        private CFrontRecordset OpenTable(string tableName)
        {
            int tableNo = this.TableNo(tableName);
            return this.OpenTable(tableNo);
        }

        private void SelectLatestVersion()
        {
            CFrontDll.SelectLatestVersion();
        }

        private int TableNo(string tableName)
        {
            return CFrontDll.TableNo(tableName);
        }

        private bool usuarioValido()
        {
            int num;
            if (this.UsuarioId.Length > 80)
            {
                return false;
            }
            if (this.UsuarioId == "")
            {
                return false;
            }
            for (num = 0; num < this.UsuarioId.Length; num++)
            {
                if ((((this.UsuarioId[num] == '*') || (this.UsuarioId[num] == '<')) || ((this.UsuarioId[num] == '>') || (this.UsuarioId[num] == '@'))) || ((this.UsuarioId[num] == '|') || (this.UsuarioId[num] == '&')))
                {
                    return false;
                }
            }
            if (this.Contraseña.Length > 30)
            {
                return false;
            }
            if (this.Contraseña == "")
            {
                return false;
            }
            for (num = 0; num < this.Contraseña.Length; num++)
            {
                if ((((this.Contraseña[num] == '*') || (this.Contraseña[num] == '?')) || ((this.Contraseña[num] == '<') || (this.Contraseña[num] == '>'))) || (((this.Contraseña[num] == '@') || (this.Contraseña[num] == '|')) || (this.Contraseña[num] == '&')))
                {
                    return false;
                }
            }
            return true;
        }

        public void ValidateUser()
        {
            this.Clave = -1;
            this.Conectado = false;
            if (!this.usuarioValido())
            {
                throw new Exception("Nonvalid user.");
            }
            this.SelectLatestVersion();
            CFrontRecordset recordset = this.OpenTable("Mobile Users");
            if (NavisionDBConnection.nLT.Length < recordset.RecordCount)
            {
                throw new Exception("ERROR n\x00famero de usuarios de licencia");
            }
            recordset.SetFilter("User ID", this.UsuarioId);
            recordset.SetFilter("Password", this.Contraseña);
            recordset.MoveFirst();
            
            if (recordset.EOF)
            {
                recordset.FreeRec();
                recordset.Close();
            }
            else if (Convert.ToString(recordset["Password"]) != this.Password)
            {
                recordset.FreeRec();
                recordset.Close();
            }
            else
            {
                Random random = new Random();
                if ((Convert.ToInt32(recordset["Key"]) == 0) && !Convert.ToBoolean(recordset["Connected"]))
                {
                    this.Clave = random.Next();
                }
                else
                {
                    this.Clave = Convert.ToInt32(recordset["Key"]);
                }
                this.UsuarioCode = Convert.ToString(recordset["User Code"]);
                this.Nombre_usuario = Convert.ToString(recordset["User Name"]);
                this.Tipo_Usuario = Convert.ToString(recordset["User Type"]);
                this.NivelDeRestriccion = Convert.ToString(recordset["Restriction Level"]);
                this.CorreoE = Convert.ToString(recordset["Email"]);
                this.FechaHoraConexion = DateTime.Now;
                this.numConexiones = Convert.ToInt32(recordset["Connection Times Counter"]) + 1;
                recordset.FreeRec();
                recordset.Close();
                this.Conectado = true;
            }
        }

        public bool Connected
        {
            get
            {
                return this.Conectado;
            }
        }

        public DateTime Connection_DateTime
        {
            get
            {
                return this.FechaHoraConexion;
            }
        }

        public int Connection_Times_Counter
        {
            get
            {
                return this.numConexiones;
            }
        }

        public DataSet Dataset
        {
            get
            {
                DataSet set = new DataSet();
                DataTable table = new DataTable("Login");
                DataColumn column = new DataColumn("Connected", Type.GetType("System.Boolean"));
                table.Columns.Add(column);
                column = new DataColumn("UserId", Type.GetType("System.String"));
                table.Columns.Add(column);
                column = new DataColumn("UserCode", Type.GetType("System.String"));
                table.Columns.Add(column);
                column = new DataColumn("UserName", Type.GetType("System.String"));
                table.Columns.Add(column);
                column = new DataColumn("UserType", Type.GetType("System.String"));
                table.Columns.Add(column);
                column = new DataColumn("RestrictionLevel", Type.GetType("System.String"));
                table.Columns.Add(column);
                column = new DataColumn("Email", Type.GetType("System.String"));
                table.Columns.Add(column);
                column = new DataColumn("ConnectionDateTime", Type.GetType("System.DateTime"));
                table.Columns.Add(column);
                column = new DataColumn("ConnectionTimesCounter", Type.GetType("System.Int32"));
                table.Columns.Add(column);
                DataRow row = table.NewRow();
                if (this.Conectado)
                {
                    row[0] = true;
                    row[1] = this.UsuarioId;
                    row[2] = this.UserCode;
                    row[3] = this.Nombre_usuario;
                    row[4] = this.Tipo_Usuario;
                    row[5] = this.NivelDeRestriccion;
                    row[6] = this.CorreoE;
                    row[7] = this.FechaHoraConexion;
                    row[8] = this.numConexiones;
                }
                else
                {
                    row[0] = false;
                }
                table.Rows.Add(row);
                table.AcceptChanges();
                set.Tables.Add(table);
                set.AcceptChanges();
                return set;
            }
        }

        public string Email
        {
            get
            {
                return this.CorreoE;
            }
        }

        public string Password
        {
            get
            {
                return this.Contraseña;
            }
            set
            {
                this.Contraseña = value;
            }
        }

        public string RestrictionLevel
        {
            get
            {
                return this.NivelDeRestriccion;
            }
        }

        public string UserCode
        {
            get
            {
                return this.UsuarioCode;
            }
        }

        public string UserId
        {
            get
            {
                return this.UsuarioId;
            }
            set
            {
                this.UsuarioId = value;
            }
        }

        public int UserKey
        {
            get
            {
                return this.Clave;
            }
        }

        public string UserName
        {
            get
            {
                return this.Nombre_usuario;
            }
        }

        public string UserType
        {
            get
            {
                return this.Tipo_Usuario;
            }
        }
    }
}

