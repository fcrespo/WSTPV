namespace NavisionDB
{
    using Microsoft.Navision.CFront;
    using System;
    using System.Collections;
    using System.Runtime.CompilerServices;
    using System.Runtime.InteropServices;
    using System.Text;

    internal class CFrontDll
    {
        private static NavisionDriverType cf_driver;
        private static string cf_path;
        private static CFrontDotNet Cfront;

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static int AllocRec(int hTable)
        {
            return Cfront.AllocRecord(hTable);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Allow(NavisionAllowedError ErrorCode)
        {
            Cfront.Allow(ErrorCode);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void AllowAll()
        {
            Cfront.Allow(NavisionAllowedError.KeyNotFound);
            Cfront.Allow(NavisionAllowedError.RecordExists);
            Cfront.Allow(NavisionAllowedError.TableNotFound);
            Cfront.Allow(NavisionAllowedError.TableNotFound);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void AWT()
        {
            Cfront.AbortWriteTransaction();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void BWT()
        {
            Cfront.BeginWriteTransaction();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void CalcFields(int hTable, int hRec, int[] fields)
        {
            Cfront.CalcFields(hTable, hRec, fields);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void CalcSums(int hTable, int hRec, int[] fields)
        {
            Cfront.CalcSums(hTable, hRec, fields);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void CalcSums(int hTable, int hRec, TableKey key)
        {
            Cfront.CalcSums(hTable, hRec, key.sumFields);
        }

        public static void CFrontIni(NavisionDriverType driver, string path)
        {
            cf_driver = driver;
            cf_path = path;
            try
            {
                Cfront = new CFrontDotNet(driver, path);
            }
            catch (Exception exception)
            {
                throw exception;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void CheckLicenseFile(int ObjectNo)
        {
            Cfront.CheckLicenseFile(ObjectNo);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void CloseCompany()
        {
            Cfront.CloseCompany();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void CloseDatabase()
        {
            Cfront.CloseDatabase();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void CloseTable(int hTable)
        {
            Cfront.CloseTable(hTable);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void ConnectServerandOpenDataBase(string ServerName, NavisionNetType NetType, string DatabaseName, int CacheSize, bool UseCommitCache, bool UseNTAuthentication, string UserID, string Password)
        {
            Cfront.ConnectServerAndOpenDatabase(ServerName, NetType, DatabaseName, CacheSize, UseCommitCache, UseNTAuthentication, UserID, Password);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void CryptPassword(string UserID, [In, Out] string Password)
        {
            Cfront.CryptPassword(UserID.ToString(), Password.ToString());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void DeleteRec(int hTable, int hRec)
        {
            Cfront.Allow(NavisionAllowedError.TableNotFound);
            if (!Cfront.DeleteRecord(hTable, hRec))
            {
                throw new Exception("ERROR: An error ocurred while trying to delete a record");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void DeleteRecs(int hTable)
        {
            Cfront.Allow(NavisionAllowedError.TableNotFound);
            try
            {
                Cfront.DeleteRecords(hTable);
            }
            catch
            {
                throw new Exception("ERROR: An error ocurred while trying to delete a record");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void DisconnectServer()
        {
            Cfront.DisconnectServer();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static string eliminateFilterOperators(string filter)
        {
            return filter.Replace("..", "").Replace("&", "").Replace("|", "").Replace("<", "").Replace(">", "").Replace("=", "").Replace("*", "").Replace("?", "").Replace("'", "");
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void ErrorHandler(int errCode, bool isFatal)
        {
            throw new Exception("Error code: " + errCode.ToString() + " Is fatal: " + isFatal.ToString());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void EWT()
        {
            Cfront.Allow(NavisionAllowedError.None);
            Cfront.EndWriteTransaction();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static NavisionFieldClass fieldClass(int hTable, int fieldNo)
        {
            return Cfront.FieldClass(hTable, fieldNo);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static short FieldCount(int hTable)
        {
            return Cfront.FieldCount(hTable);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static string FieldName(int hTable, int fieldNo)
        {
            return Cfront.FieldName(hTable, fieldNo);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static int FieldNo(int hTable, string fieldName)
        {
            return Cfront.FieldNo(hTable, fieldName);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static string[] FieldOptions(int hTable, int fieldNo)
        {
            if (FieldType(hTable, fieldNo) != NavisionFieldType.Option)
            {
                throw new Exception("Options are only valid for Option fields.");
            }
            return Cfront.FieldOption(hTable, fieldNo).Split(new char[] { ',' });
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static string fieldType(int hTable, int fieldNo, ref NavisionFieldType fieldType)
        {
            fieldType = Cfront.FieldType(hTable, fieldNo);
            return ((NavisionFieldType) fieldType).ToString();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static NavisionFieldType FieldType(int hTable, int fieldNo)
        {
            return Cfront.FieldType(hTable, fieldNo);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static bool FindRec(int hTable, int hRec, string searchMethod)
        {
            if (RecordCount(hTable) == 0)
            {
                return false;
            }
            return Cfront.FindRecord(hTable, hRec, searchMethod);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void FreeRec(int hRec)
        {
            Cfront.FreeRecord(hRec);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static TableKey[] GetAllKeys(int hTable)
        {
            int[] key = null;
            int num = Cfront.KeyCount(hTable);
            TableKey[] keyArray = new TableKey[num];
            for (int i = 0; i < num; i++)
            {
                key = Cfront.NextKey(hTable, key);
                keyArray[i] = KeyFromPointer(key, hTable);
            }
            return keyArray;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static byte[] GetBytesByType(NavisionFieldType type, object value, int tableHandle, int fieldNo)
        {
            NavisionFieldType type2 = type;
            if (type2 <= NavisionFieldType.Blob)
            {
                switch (type2)
                {
                    case NavisionFieldType.TableFilter:
                        return NavisionTableFilter.Parse(Convert.ToString(value)).GetBytes();

                    case NavisionFieldType.RecordId:
                        return NavisionRecordId.Parse(Convert.ToString(value)).GetBytes();

                    case NavisionFieldType.Text:
                        return NavisionText.Parse((string) value).GetBytes();

                    case NavisionFieldType.Binary:
                        return NavisionBinary.Parse(Convert.ToString(value)).GetBytes();

                    case ((NavisionFieldType) 0x8401):
                        goto Label_0322;

                    case NavisionFieldType.Blob:
                        return new byte[0];

                    case NavisionFieldType.Decimal:
                        return NavisionDecimal.Parse(Convert.ToString(value)).GetBytes();

                    case NavisionFieldType.Date:
                    {
                        NavisionDate date = new NavisionDate();
                        if (value.ToString() != "")
                        {
                            date = NavisionDate.Parse(Convert.ToDateTime(value).ToShortDateString());
                        }
                        return date.GetBytes();
                    }
                    case NavisionFieldType.Time:
                        return NavisionTime.Parse(Convert.ToDateTime(value).ToShortTimeString()).GetBytes();

                    case NavisionFieldType.DateFormula:
                        return NavisionDateFormula.Parse(Convert.ToString(value)).GetBytes();
                }
            }
            else
            {
                switch (type2)
                {
                    case NavisionFieldType.Code:
                        return NavisionCode.Parse(Convert.ToString(value)).GetBytes();

                    case NavisionFieldType.Option:
                    {
                        NavisionOption option = new NavisionOption(0);
                        if (value.GetType() != Type.GetType("System.String"))
                        {
                            option = new NavisionOption(Convert.ToInt32((string) value));
                        }
                        else
                        {
                            string[] strArray = FieldOptions(tableHandle, fieldNo);
                            for (int i = 0; i < strArray.Length; i++)
                            {
                                if (strArray[i] == ((string) value))
                                {
                                    option = new NavisionOption(i);
                                    break;
                                }
                            }
                        }
                        return option.GetBytes();
                    }
                    case NavisionFieldType.Boolean:
                        return NavisionBoolean.Parse(Convert.ToString(value)).GetBytes();

                    case NavisionFieldType.Integer:
                    {
                        NavisionInteger integer2 = new NavisionInteger(Convert.ToInt32(value));
                        return integer2.GetBytes();
                    }
                    case NavisionFieldType.BigInteger:
                        return NavisionBigInteger.Parse(Convert.ToString(value)).GetBytes();

                    case NavisionFieldType.Duration:
                        return NavisionDuration.Parse(Convert.ToString(value)).GetBytes();

                    case NavisionFieldType.Guid:
                        return NavisionGuid.Parse(Convert.ToString(value)).GetBytes();

                    case NavisionFieldType.DateTime:
                    {
                        NavisionDateTime time = new NavisionDateTime();
                        if (value.ToString() != "")
                        {
                            time = NavisionDateTime.Parse(Convert.ToDateTime(value).ToString());
                        }
                        return time.GetBytes();
                    }
                }
            }
        Label_0322:
            return new byte[0];
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static TableKey GetCurrentKey(int hTable)
        {
            return KeyFromPointer(Cfront.GetCurrentKey(hTable), hTable);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static object GetFieldValue(int hRec, int hTable, int fieldNo)
        {
            NavisionFieldType type = Cfront.FieldType(hTable, fieldNo);
            object obj2 = new object();
            new StringBuilder(0xff);
            if (Cfront.FieldClass(hTable, fieldNo) == NavisionFieldClass.FlowField)
            {
                int[] fields = new int[] { fieldNo, 0 };
                CalcFields(hTable, hRec, fields);
            }
            obj2 = Cfront.GetFieldData(hTable, hRec, fieldNo);
            object valueAsObject = GetValueAsObject((NavisionValue) obj2);
            if ((type == NavisionFieldType.Option) && (((string) valueAsObject) != ""))
            {
                valueAsObject = FieldOptions(hTable, fieldNo)[Convert.ToInt32(valueAsObject)];
            }
            return valueAsObject;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void GetFilter(int hTable, int fieldNo, out string filterExpr)
        {
            filterExpr = Cfront.GetFilter(hTable, fieldNo);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static int[] GetKeyAt(int hTable, ushort index)
        {
            int[] key = null;
            int num = Cfront.KeyCount(hTable);
            if (index >= num)
            {
                throw new Exception("Nonvalid key index.");
            }
            for (int i = 0; i <= index; i++)
            {
                key = Cfront.NextKey(hTable, key);
            }
            return key;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static int[] GetKeySumFields(int hTable, int[] keyPtr)
        {
            try
            {
                int[] numArray = Cfront.KeySqlIndexFields(hTable, keyPtr);
                new ArrayList();
                if (numArray == null)
                {
                    return new int[0];
                }
                int[] numArray2 = new int[keyPtr.Length];
                for (int i = 0; i < numArray2.Length; i++)
                {
                    numArray2[i] = keyPtr[i];
                }
                return numArray2;
            }
            catch
            {
                return null;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static object GetValueAsObject(NavisionValue value)
        {
            NavisionDateTime time2;
            NavisionTime time4;
            NavisionDateFormula formula;
            NavisionFieldType fieldType = value.FieldType;
            if (fieldType <= NavisionFieldType.Blob)
            {
                switch (fieldType)
                {
                    case NavisionFieldType.TableFilter:
                    {
                        NavisionTableFilter filter = value;
                        return filter.Value;
                    }
                    case NavisionFieldType.RecordId:
                    {
                        NavisionRecordId id = value;
                        return id.Value;
                    }
                    case NavisionFieldType.Text:
                    {
                        NavisionText text = value;
                        return text.Value;
                    }
                    case NavisionFieldType.Binary:
                    {
                        NavisionBinary binary = value;
                        return binary.ToString();
                    }
                    case ((NavisionFieldType) 0x8401):
                        goto Label_02E6;

                    case NavisionFieldType.Blob:
                        return "";

                    case NavisionFieldType.Decimal:
                    {
                        NavisionDecimal num = value;
                        return Convert.ToDecimal(num.ToDouble());
                    }
                    case NavisionFieldType.Date:
                    {
                        NavisionDate date = value;
                        try
                        {
                            int[] numArray = date.ToYearMonthDayClosing();
                            return new DateTime(numArray[0], numArray[1], numArray[2]);
                        }
                        catch
                        {
                            return DBNull.Value;
                        }
                        goto Label_01E6;
                    }
                    case NavisionFieldType.Time:
                        goto Label_0231;

                    case NavisionFieldType.DateFormula:
                        goto Label_025D;
                }
            }
            else
            {
                switch (fieldType)
                {
                    case NavisionFieldType.Code:
                    {
                        NavisionCode code = value;
                        return code.ToString();
                    }
                    case NavisionFieldType.Option:
                    {
                        NavisionOption option = value;
                        return option.ToString();
                    }
                    case NavisionFieldType.Boolean:
                    {
                        NavisionBoolean flag = value;
                        return flag.Value;
                    }
                    case NavisionFieldType.Integer:
                    {
                        NavisionInteger integer2 = value;
                        return integer2.Value;
                    }
                    case NavisionFieldType.BigInteger:
                    {
                        NavisionBigInteger integer = value;
                        return integer.ToInt64();
                    }
                    case NavisionFieldType.Duration:
                    {
                        NavisionDuration duration = value;
                        return duration.Value;
                    }
                    case NavisionFieldType.Guid:
                    {
                        NavisionGuid guid = value;
                        return guid.Value;
                    }
                    case NavisionFieldType.DateTime:
                        goto Label_01E6;
                }
            }
            goto Label_02E6;
        Label_01E6:
            time2 = value;
            try
            {
                int[] numArray2 = time2.ToYearMonthDayHourMinuteSecondThousandth();
                return new DateTime(numArray2[0], numArray2[1], numArray2[2], numArray2[3], numArray2[4], numArray2[5]);
            }
            catch
            {
                return DBNull.Value;
            }
        Label_0231:
            time4 = value;
            try
            {
                return time4.Value;
            }
            catch
            {
                return DBNull.Value;
            }
        Label_025D:
            formula = value;
            return formula.Value;
        Label_02E6:
            return "";
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static string GetValueAsText(NavisionValue value)
        {
            NavisionFieldType fieldType = value.FieldType;
            if (fieldType <= NavisionFieldType.Blob)
            {
                switch (fieldType)
                {
                    case NavisionFieldType.TableFilter:
                    {
                        NavisionTableFilter filter = value;
                        return filter.ToString();
                    }
                    case NavisionFieldType.RecordId:
                    {
                        NavisionRecordId id = value;
                        return id.ToString();
                    }
                    case NavisionFieldType.Text:
                    {
                        NavisionText text = value;
                        return text.ToString();
                    }
                    case NavisionFieldType.Binary:
                    {
                        NavisionBinary binary = value;
                        return binary.ToString();
                    }
                    case ((NavisionFieldType) 0x8401):
                        goto Label_028B;

                    case NavisionFieldType.Blob:
                        return "";

                    case NavisionFieldType.Decimal:
                    {
                        NavisionDecimal num = value;
                        return num.ToString();
                    }
                    case NavisionFieldType.Date:
                    {
                        NavisionDate date = value;
                        return date.ToString();
                    }
                    case NavisionFieldType.Time:
                    {
                        NavisionTime time2 = value;
                        return time2.ToString();
                    }
                    case NavisionFieldType.DateFormula:
                    {
                        NavisionDateFormula formula = value;
                        return formula.ToString();
                    }
                }
            }
            else
            {
                switch (fieldType)
                {
                    case NavisionFieldType.Code:
                    {
                        NavisionCode code = value;
                        return code.ToString();
                    }
                    case NavisionFieldType.Option:
                    {
                        NavisionOption option = value;
                        return option.ToString();
                    }
                    case NavisionFieldType.Boolean:
                    {
                        NavisionBoolean flag = value;
                        return flag.ToString();
                    }
                    case NavisionFieldType.Integer:
                    {
                        NavisionInteger integer2 = value;
                        return integer2.ToString();
                    }
                    case NavisionFieldType.BigInteger:
                    {
                        NavisionBigInteger integer = value;
                        return integer.ToString();
                    }
                    case NavisionFieldType.Duration:
                    {
                        NavisionDuration duration = value;
                        return duration.ToString();
                    }
                    case NavisionFieldType.Guid:
                    {
                        NavisionGuid guid = value;
                        return guid.ToString();
                    }
                    case NavisionFieldType.DateTime:
                    {
                        NavisionDateTime time = value;
                        return time.ToString();
                    }
                }
            }
        Label_028B:
            return "";
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void InitRec(int hTable, int hRec)
        {
            Cfront.InitRecord(hTable, hRec);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void InsertRec(int hTable, int hRec)
        {
            Cfront.InsertRecord(hTable, hRec);
            /*
            Cfront.Allow(NavisionAllowedError.RecordExists);
            if (!Cfront.InsertRecord(hTable, hRec))
            {
                throw new Exception("ERROR: An error ocurred while trying to insert a record");
            }
             */
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static TableKey KeyFromPointer(int[] keyPtr, int hTable)
        {
            ArrayList list = new ArrayList();
            for (int i = 0; i < keyPtr.Length; i++)
            {
                list.Add(i);
            }
            int[] numArray = new int[keyPtr.Length + 1];
            for (int j = 0; j < (numArray.Length - 1); j++)
            {
                numArray[j] = keyPtr[j];
            }
            numArray[numArray.Length - 1] = 0;
            return new TableKey(keyPtr, GetKeySumFields(hTable, keyPtr));
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void LockTable(int hTable, NavisionTableLockMode Mode)
        {
            Cfront.LockTable(hTable, Mode);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void ModifyRec(int hTable, int hRec)
        {
            Cfront.Allow(NavisionAllowedError.TableNotFound);
            if (!Cfront.ModifyRecord(hTable, hRec))
            {
                throw new Exception("ERROR: An error ocurred while trying to modify a record");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static string NextCompany(string company)
        {
            return Cfront.NextCompany(company);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static int NextField(int hTable, int fieldNo)
        {
            return Cfront.NextField(hTable, fieldNo);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static short NextRec(int hTable, int hRec, short step)
        {
            return Cfront.NextRecord(hTable, hRec, step);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void OpenCompany(string company)
        {
            Cfront.OpenCompany(company);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static bool OpenTable(out int hTable, int tableNo)
        {
            Cfront.Allow(NavisionAllowedError.TableNotFound);
            if ((hTable = Cfront.OpenTable(tableNo)) != 0)
            {
                return false;
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void OpenTable(out int hTable, string tableName)
        {
            int tableNo = TableNo(tableName);
            OpenTable(out hTable, tableNo);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static bool PonerComillas(string Cadena, NavisionFieldType fieldType)
        {
            if (Cadena == "")
            {
                return false;
            }
            if (fieldType == NavisionFieldType.Boolean)
            {
                return false;
            }
            if (Cadena.IndexOf("<>") > -1)
            {
                return false;
            }
            if (Cadena.IndexOf("..") > -1)
            {
                return false;
            }
            for (int i = 0; i < Cadena.Length; i++)
            {
                if (((Cadena[i] == '<') || (Cadena[i] == '>')) || (((Cadena[i] == '=') || (Cadena[i] == '|')) || (Cadena[i] == '&')))
                {
                    if (((Cadena[i] == '<') || (Cadena[i] == '>')) && ((fieldType != NavisionFieldType.Text) && (fieldType != NavisionFieldType.Code)))
                    {
                        return false;
                    }
                    if (((Cadena[i] == '|') || (Cadena[i] == '&')) && ((fieldType == NavisionFieldType.Text) || (fieldType == NavisionFieldType.Code)))
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static int RecordCount(int hTable)
        {
            int num;
            try
            {
                num = Cfront.RecordCount(hTable);
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message);
            }
            return num;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SelectLatestVersion()
        {
            Cfront.SelectLatestVersion();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SetCurrentKey(int hTable, TableKey key)
        {
            SetCurrentKey(hTable, key.fields);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SetCurrentKey(int hTable, int[] fields)
        {
            if (fields == null)
            {
                fields = new int[0];
            }
            TableKey[] allKeys = GetAllKeys(hTable);
            int num = -1;
            for (int i = 0; i < allKeys.Length; i++)
            {
                TableKey key = allKeys[i];
                if (key.fields.Length == fields.Length)
                {
                    bool flag = true;
                    for (int j = 0; j < fields.Length; j++)
                    {
                        if (key.fields[j] != fields[j])
                        {
                            flag = false;
                            break;
                        }
                    }
                    if (flag)
                    {
                        num = i;
                        break;
                    }
                }
            }
            if ((fields.Length > 0) && (num == -1))
            {
                string str = "{";
                for (int k = 0; k < fields.Length; k++)
                {
                    str = str + "," + fields[k].ToString();
                }
                throw new Exception("Key not found: " + str + "}");
            }
            int[] keyAt = null;
            if (num >= 0)
            {
                keyAt = GetKeyAt(hTable, (ushort) num);
            }
            Cfront.SetCurrentKey(hTable, keyAt);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SetFieldValue(int hRec, int hTable, int fieldNo, object data)
        {
            NavisionFieldType fieldType = Cfront.FieldType(hTable, fieldNo);
            if ((Cfront.FieldClass(hTable, fieldNo) != NavisionFieldClass.FlowField) && !(data is DBNull))
            {
                Cfront.SetFieldData(hTable, hRec, fieldNo, fieldType, GetBytesByType(fieldType, data, hTable, fieldNo));
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void SetFilter(int hTable, int fieldNo, string filterExpr)
        {
            NavisionFieldType fieldType = Cfront.FieldType(hTable, fieldNo);
            string message = "";
            if ((filterExpr != "") && (filterExpr != "''"))
            {
                message = ValidateField(hTable, fieldNo, fieldType, filterExpr);
            }
            switch (fieldType)
            {
                case NavisionFieldType.DateTime:
                case NavisionFieldType.Date:
                case NavisionFieldType.Time:
                case NavisionFieldType.Decimal:
                    break;

                case NavisionFieldType.Option:
                    int num;
                    try
                    {
                        num = Convert.ToInt32(filterExpr);
                    }
                    catch
                    {
                        if (filterExpr == "")
                        {
                            break;
                        }
                        string str2 = filterExpr.ToUpper();
                        char[] separator = new char[] { '|', '&' };
                        string[] strArray = str2.Split(separator);
                        if (strArray.Length == 0)
                        {
                            strArray = new string[] { str2 };
                        }
                        string[] strArray2 = new string[strArray.Length];
                        int[] numArray = new int[strArray.Length];
                        string[] strArray3 = FieldOptions(hTable, fieldNo);
                        num = -1;
                        for (int i = 0; i < strArray.Length; i++)
                        {
                            bool flag = false;
                            for (int k = 0; k < strArray3.Length; k++)
                            {
                                string str3;
                                if (!(strArray3[k].ToUpper() == ""))
                                {
                                    str3 = strArray3[k].ToUpper();
                                }
                                else
                                {
                                    str3 = "";
                                }
                                if (strArray[i].Replace("<", "").Replace(">", "").Equals(str3) || strArray[i].Equals(k.ToString()))
                                {
                                    flag = true;
                                    strArray2[i] = strArray[i];
                                    numArray[i] = k;
                                    break;
                                }
                            }
                            if (!flag)
                            {
                                num = -1;
                                break;
                            }
                            num = 0;
                        }
                        if ((num == -1) && (str2 != ""))
                        {
                            throw new Exception(string.Concat(new object[] { "Nonvalid option text specified \"", str2, "\" for field ", fieldNo }));
                        }
                        filterExpr = "";
                        int startIndex = 0;
                        for (int j = 0; j < strArray.Length; j++)
                        {
                            filterExpr = filterExpr + strArray2[j].Replace(strArray2[j].Replace("<", "").Replace(">", ""), numArray[j].ToString());
                            string str4 = "";
                            startIndex += strArray[j].Length;
                            if (startIndex < (str2.Length - 1))
                            {
                                str4 = str2.Substring(startIndex, 1);
                                if (str4 != "")
                                {
                                    filterExpr = filterExpr + str4;
                                    startIndex++;
                                }
                            }
                        }
                        if ((strArray.Length == 1) && (strArray[0].IndexOf("<", 0, strArray.Length) < 0))
                        {
                            filterExpr = "'" + filterExpr + "'";
                        }
                    }
                    break;

                default:
                    if (fieldType == NavisionFieldType.Boolean)
                    {
                        try
                        {
                            if ((filterExpr != "0") && (filterExpr != "1"))
                            {
                                if (Convert.ToBoolean(filterExpr))
                                {
                                    filterExpr = "1";
                                }
                                else
                                {
                                    filterExpr = "0";
                                }
                            }
                            break;
                        }
                        catch
                        {
                            throw new Exception("ERROR: Data type not match in field n\x00ba " + fieldNo.ToString());
                        }
                    }
                    if (PonerComillas(filterExpr, fieldType))
                    {
                        filterExpr = "'" + filterExpr + "'";
                    }
                    break;
            }
            if (message != "")
            {
                throw new Exception(message);
            }
            Cfront.SetFilter(hTable, fieldNo, filterExpr);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static string TableName(int hTable)
        {
            Cfront.Allow(NavisionAllowedError.TableNotFound);
            return Cfront.TableName(hTable);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static int TableNo(string tableName)
        {
            Allow(NavisionAllowedError.TableNotFound);
            return Cfront.TableNo(tableName.ToString());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static string UserID()
        {
            return Cfront.UserId;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static string ValidateField(int hTable, int fieldNo, NavisionFieldType fieldType, string filter)
        {
            if (filter.Length > 250)
            {
                return "ERROR: Filter size exceeds the maximun length allowed";
            }
            char[] separator = new char[] { '|', '&' };
            string[] strArray = filter.Split(separator);
            int length = 0;
            for (int i = 0; i < strArray.Length; i++)
            {
                string str;
                int num4;
                length = eliminateFilterOperators(strArray[i]).Length;
                NavisionFieldType type = fieldType;
                if (type <= NavisionFieldType.Blob)
                {
                    switch (type)
                    {
                        case NavisionFieldType.TableFilter:
                            return "";

                        case NavisionFieldType.RecordId:
                            goto Label_0417;

                        case NavisionFieldType.Text:
                            goto Label_046F;

                        case NavisionFieldType.Binary:
                            goto Label_0181;

                        case ((NavisionFieldType) 0x8401):
                        {
                            continue;
                        }
                        case NavisionFieldType.Blob:
                            return ("ERROR: Cannot filter a BLOB field. Field n\x00ba " + fieldNo.ToString());

                        case NavisionFieldType.Decimal:
                            try
                            {
                                Convert.ToDecimal(eliminateFilterOperators(strArray[i]));
                                continue;
                            }
                            catch
                            {
                                return ("ERROR: Data type not match in field n\x00ba " + fieldNo.ToString());
                            }
                            goto Label_0181;

                        case NavisionFieldType.Date:
                            goto Label_019F;

                        case NavisionFieldType.Time:
                            goto Label_04A0;

                        case NavisionFieldType.DateFormula:
                            goto Label_03FF;
                    }
                }
                else
                {
                    switch (type)
                    {
                        case NavisionFieldType.Boolean:
                            return "";

                        case NavisionFieldType.Integer:
                            goto Label_0443;

                        case NavisionFieldType.Code:
                            goto Label_04CC;

                        case NavisionFieldType.Guid:
                            return "";

                        case NavisionFieldType.DateTime:
                            goto Label_019F;

                        case NavisionFieldType.Option:
                            return "";

                        case NavisionFieldType.Duration:
                            return "";
                    }
                }
                continue;
            Label_0181:
                return "";
            Label_019F:
                str = "  ... ";
                if ((strArray[i] == " ") || (strArray[i] == "''"))
                {
                    strArray[i] = "''";
                    continue;
                }
                try
                {
                    int num3 = strArray[i].IndexOf("..");
                    if (num3 != -1)
                    {
                        string str2 = strArray[i].Substring(0, num3);
                        string str3 = strArray[i].Substring(num3 + 2, (strArray[i].Length - num3) - 2);
                        if (str2 != "")
                        {
                            if (str2.IndexOf("/") != -1)
                            {
                                str = str + "substr1:" + str2;
                                Convert.ToDateTime(str2);
                                str = str + "...";
                            }
                            else
                            {
                                str2 = str2.Insert(2, "/").Insert(5, "/");
                                str = str + "substr1(2):" + str2;
                                Convert.ToDateTime(str2);
                                str = str + "...";
                            }
                        }
                        if (str3 != "")
                        {
                            if (str3.IndexOf("/") != -1)
                            {
                                str = str + "substr2:" + str3;
                                Convert.ToDateTime(str3);
                                str = str + "...";
                            }
                            else
                            {
                                str3 = str3.Insert(2, "/").Insert(5, "/");
                                str = str + "substr2(2):" + str3;
                                Convert.ToDateTime(str3);
                                str = str + "...";
                            }
                        }
                    }
                    else
                    {
                        strArray[i] = strArray[i].Replace("<", "").Replace(">", "").Replace("=", "");
                        if (strArray[i].IndexOf("/") != -1)
                        {
                            str = str + "granulos[i]:" + strArray[i];
                            Convert.ToDateTime(strArray[i]);
                            str = str + "...";
                        }
                        else
                        {
                            strArray[i] = strArray[i].Insert(2, "/").Insert(5, "/");
                            str = str + "granulos[i](2):" + strArray[i];
                            Convert.ToDateTime(strArray[i]);
                            str = str + "...";
                        }
                    }
                    continue;
                }
                catch (Exception exception)
                {
                    string message = exception.Message;
                    return ("ERROR(2): Data type not match in field n\x00ba " + fieldNo.ToString() + str);
                }
            Label_03FF:
                return "";
            Label_0417:
                try
                {
                    Convert.ToInt32(eliminateFilterOperators(strArray[i]));
                    continue;
                }
                catch
                {
                    return ("ERROR: Data type not match in field n\x00ba " + fieldNo.ToString());
                }
            Label_0443:
                try
                {
                    Convert.ToInt32(eliminateFilterOperators(strArray[i]));
                    continue;
                }
                catch
                {
                    return ("ERROR: Data type not match in field n\x00ba " + fieldNo.ToString());
                }
            Label_046F:
                if ((Cfront.FieldLength(hTable, fieldNo) + 1) >= length)
                {
                    continue;
                }
                return ("ERROR: Filter size in field n\x00ba " + fieldNo.ToString() + " exceeds the field size");
            Label_04A0:
                try
                {
                    Convert.ToDateTime(eliminateFilterOperators(strArray[i]));
                    continue;
                }
                catch
                {
                    return ("ERROR: Data type not match in field n\x00ba " + fieldNo.ToString());
                }
            Label_04CC:
                num4 = Cfront.FieldLength(hTable, fieldNo);
                int index = strArray[i].IndexOf("..");
                if (index != -1)
                {
                    string str4 = strArray[i].Substring(0, index);
                    string str5 = strArray[i].Substring(index + 2, (strArray[i].Length - index) - 2);
                    if ((str4 != "") && (str4.Length > num4))
                    {
                        return ("ERROR: Filter size in field n\x00ba " + fieldNo.ToString() + " ( " + str4 + ") exceeds the field size");
                    }
                    if ((str5 != "") && (str5.Length > num4))
                    {
                        return ("ERROR: Filter size in field n\x00ba " + fieldNo.ToString() + " ( " + str5 + ") exceeds the field size");
                    }
                }
                else if (strArray[i].Length > num4)
                {
                    return ("ERROR: Filter size in field n\x00ba " + fieldNo.ToString() + " ( " + strArray[i] + ") exceeds the field size");
                }
                return "";
            }
            return "";
        }
    }
}

