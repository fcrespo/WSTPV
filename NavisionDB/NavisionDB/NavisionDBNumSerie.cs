namespace NavisionDB
{
    using System;

    public class NavisionDBNumSerie
    {
        private NavisionDBConnection DBConnection;
        private NavisionDBUser DBUser;

        public NavisionDBNumSerie(NavisionDBUser DBUser, NavisionDBConnection DBConnection)
        {
            this.DBConnection = DBConnection;
            this.DBUser = DBUser;
        }

        private string NumeroSerie_Generar(string ultimoNumero, int incremento)
        {
            string numero = ultimoNumero;
            for (int i = 1; i < (incremento + 1); i++)
            {
                numero = this.NumeroSerie_ObtenerSiguiente(numero);
            }
            return numero;
        }

        private string NumeroSerie_ObtenerSiguiente(string numero)
        {
            string str2 = "";
            string str3 = "";
            bool flag = false;
            string str = numero.Trim().ToUpper();
            do
            {
                str3 = Convert.ToString(str[str.Length - 1]);
                str = str.Substring(0, str.Length - 1);
                if ((str3.CompareTo("0") >= 0) && (str3.CompareTo("9") <= 0))
                {
                    if (Convert.ToInt32(str3) == 9)
                    {
                        str2 = "0" + str2.Trim();
                    }
                    else
                    {
                        flag = true;
                        str = str.Trim() + Convert.ToString((int) (Convert.ToInt16(str3) + 1));
                    }
                }
                else if ((str3.CompareTo("0") >= 0) && (str3.CompareTo("9") <= 0))
                {
                    str2 = str3.Trim() + "1" + str2.Trim();
                    flag = true;
                }
                else
                {
                    str2 = Convert.ToString((int) (Convert.ToUInt16(str3) + Convert.ToUInt16(str2)));
                    if (str == "")
                    {
                        flag = true;
                    }
                }
            }
            while ((str != "") && !flag);
            return (str.Trim() + str2.Trim());
        }

        public string ObtainSeriesNumber(string seriesCode)
        {
            CFrontRecordset recordset = null;
            string str6;
            string str = "";
            try
            {
                if (this.ValidarUsuario(this.DBUser))
                {
                    this.DBConnection.SelectLatestVersion();
                    recordset = this.DBConnection.OpenTable(0x135);
                    int[] fields = new int[] { 1, 3, 4 };
                    recordset.SetCurrentKey(fields);
                    string str2 = "";
                    str2 = DateTime.Now.ToString("dd/MM/yyyy");
                    recordset.SetFilter(1, seriesCode);
                    recordset.SetFilter(3, ".." + str2);
                    recordset.SetFilter(9, "true");
                    string str3 = null;
                    string strB = null;
                    string ultimoNumero = null;
                    int incremento = 0;
                    recordset.MoveLast();
                    this.DBConnection.BWT();
                    recordset.LockTable();
                    if (!recordset.EOF)
                    {
                        str3 = Convert.ToString(recordset[4]);
                        strB = Convert.ToString(recordset[5]);
                        incremento = Convert.ToInt32(recordset[7]);
                        ultimoNumero = Convert.ToString(recordset[8]);
                    }
                    if (incremento < 1)
                    {
                        throw new Exception("ERROR: 'Increment-by No.' It cannot be possible to change 'No. Series'.");
                    }
                    if (ultimoNumero.Trim() == "")
                    {
                        str = str3;
                    }
                    else
                    {
                        str = this.NumeroSerie_Generar(ultimoNumero, incremento);
                    }
                    if ((str.CompareTo(strB) > 0) && (strB.Trim() != ""))
                    {
                        throw new Exception("ERROR: Maximum number exceeded. It cannot be possible to change 'No. Series'.");
                    }
                    recordset.Edit();
                    recordset[8] = str;
                    recordset[10] = DateTime.Now.Date.ToString("dd/MM/yyyy");
                    recordset.Update();
                    recordset.FreeRec();
                    recordset.Close();
                    this.DBConnection.EWT();
                }
                str6 = str;
            }
            catch (Exception exception)
            {
                this.DBConnection.AWT();
                throw new Exception(exception.Message);
            }
            return str6;
        }

        private bool ValidarUsuario(NavisionDBUser DBUser)
        {
            CFrontRecordset recordset = null;
            if ((this.DBConnection == null) || (DBUser.UserId == ""))
            {
                return false;
            }
            if (DBUser.UserId != "anonymous")
            {
                this.DBConnection.SelectLatestVersion();
                recordset = this.DBConnection.OpenTable("Mobile Users");
                recordset.SetFilter("User ID", DBUser.UserId);
                recordset.SetFilter("Password", DBUser.Password);
                recordset.MoveFirst();
                if (recordset.EOF)
                {
                    recordset.FreeRec();
                    recordset.Close();
                    return false;
                }
                recordset.FreeRec();
                recordset.Close();
            }
            return true;
        }
    }
}

