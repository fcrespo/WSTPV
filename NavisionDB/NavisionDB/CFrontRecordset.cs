namespace NavisionDB
{
    using Microsoft.Navision.CFront;
    using System;
    using System.Reflection;

    internal class CFrontRecordset
    {
        protected bool editMode;
        protected int hRec;
        protected int hTable;
        protected bool isNew;
        protected bool varBOF;
        protected bool varEOF;
        protected string[] varFieldNames;
        protected int[] varFieldNumbers;

        public CFrontRecordset(int hTableLocal)
        {
            this.hTable = hTableLocal;
            this.hRec = CFrontDll.AllocRec(this.hTable);
            if (this.RecordCount == 0)
            {
                this.varEOF = true;
            }
        }

        public void AddNew()
        {
            this.editMode = true;
            this.InitRec();
            this.isNew = true;
        }

        public void CalcSums(int[] fields)
        {
            int[] array = new int[fields.Length + 1];
            fields.CopyTo(array, 0);
            array[array.Length - 1] = 0;
            CFrontDll.CalcSums(this.hTable, this.hRec, array);
        }

        public void CancelUpdate()
        {
            if (!this.editMode)
            {
                throw new Exception("To do CancelUpdate() it's necessary being in edit mode");
            }
            this.editMode = false;
            this.MoveStep(0);
        }

        protected void CheckFieldNumber(int fieldNo)
        {
            foreach (int num in this.FieldNumbers)
            {
                if (fieldNo == num)
                {
                    return;
                }
            }
            throw new Exception("Número de campo desconocido en CheckFieldNumber(rS): " + fieldNo);
        }

        protected void CheckValidRecord()
        {
            if (this.EOF)
            {
                throw new Exception("Registro no válido: End Of File (EOF)");
            }
            if (this.BOF)
            {
                throw new Exception("Registro no válido: Begin Of File (BOF)");
            }
        }

        public void Close()
        {
            CFrontDll.CloseTable(this.hTable);
            this.hTable = 0;
        }

        public void Delete()
        {
            if (this.editMode)
            {
                throw new Exception("No es posible eliminar registros en modo edición");
            }
            CFrontDll.DeleteRec(this.hTable, this.hRec);
        }

        public void DeleteAll()
        {
            if (this.editMode)
            {
                throw new Exception("No es posible eliminar registros en modo edición");
            }
            CFrontDll.DeleteRecs(this.hTable);
        }

        protected string DllFieldName(int fieldNo)
        {
            return CFrontDll.FieldName(this.hTable, fieldNo);
        }

        public void Edit()
        {
            this.CheckValidRecord();
            this.editMode = true;
            this.isNew = false;
        }

        public void EliminarFiltro(int fieldNo)
        {
            this.SetFilter(fieldNo, "");
        }

        public void EliminarFiltros()
        {
            int[] fieldNumbers = this.FieldNumbers;
            for (int i = 0; i < fieldNumbers.Length; i++)
            {
                this.SetFilter(fieldNumbers[i], "");
            }
        }

        public short FieldClass(int fieldNo)
        {
            return (short) CFrontDll.fieldClass(this.hTable, fieldNo);
        }

        public string FieldName(int fieldNo)
        {
            int[] fieldNumbers = this.FieldNumbers;
            for (int i = 0; i < fieldNumbers.Length; i++)
            {
                if (fieldNumbers[i] == fieldNo)
                {
                    return this.FieldNames[i];
                }
            }
            throw new Exception("Campo desconocido en FieldName(rS): " + fieldNo);
        }

        public int FieldNo(string fieldName)
        {
            string[] fieldNames = this.FieldNames;
            string str = fieldName.ToUpper();
            for (int i = 0; i < fieldNames.Length; i++)
            {
                if (fieldNames[i].ToUpper().Equals(str))
                {
                    return this.FieldNumbers[i];
                }
            }
            throw new Exception("Número de campo desconocido en FieldNo(rS): " + fieldName);
        }

        public string[] FieldOptions(int fieldNo)
        {
            return CFrontDll.FieldOptions(this.hTable, fieldNo);
        }

        public string FieldOptions(int fieldNo, int posOpcion)
        {
            return CFrontDll.FieldOptions(this.hTable, fieldNo)[posOpcion];
        }

        public string FieldType(int fieldNo, ref NavisionFieldType fieldType)
        {
            return CFrontDll.fieldType(this.hTable, fieldNo, ref fieldType);
        }

        public void FreeRec()
        {
            CFrontDll.FreeRec(this.hRec);
            this.hRec = 0;
        }

        public TableKey[] GetAllKeys()
        {
            return CFrontDll.GetAllKeys(this.hTable);
        }

        public TableKey GetCurrentKey()
        {
            return CFrontDll.GetCurrentKey(this.hTable);
        }

        public string GetFilter(int fieldNo)
        {
            string filterExpr = "";
            CFrontDll.GetFilter(this.hTable, fieldNo, out filterExpr);
            return filterExpr;
        }

        public string[] GetFilters()
        {
            int[] fieldNumbers = this.FieldNumbers;
            string[] strArray = new string[fieldNumbers.Length];
            for (int i = 0; i < fieldNumbers.Length; i++)
            {
                strArray.SetValue(this.GetFilter(fieldNumbers[i]), i);
            }
            return strArray;
        }

        protected void InitRec()
        {
            CFrontDll.InitRec(this.hTable, this.hRec);
        }

        public void LockTable()
        {
            CFrontDll.LockTable(this.hTable, NavisionTableLockMode.LockWait);
        }

        public void LockTable(NavisionTableLockMode Mode)
        {
            CFrontDll.LockTable(this.hTable, Mode);
        }

        public void MoveFirst()
        {
            this.varBOF = false;
            if (!this.EOF)
            {
                this.editMode = false;
                CFrontDll.FindRec(this.hTable, this.hRec, "-");
            }
        }

        public void MoveLast()
        {
            this.varBOF = false;
            if (!this.EOF)
            {
                CFrontDll.FindRec(this.hTable, this.hRec, "+");
            }
        }

        public bool MoveNext()
        {
            return (this.MoveStep(1) == 1);
        }

        public bool MovePrevious()
        {
            return (this.MoveStep(-1) == -1);
        }

        public short MoveStep(short step)
        {
            short num = CFrontDll.NextRec(this.hTable, this.hRec, step);
            if (num < step)
            {
                this.varEOF = true;
                return num;
            }
            if (num > step)
            {
                this.varBOF = true;
            }
            return num;
        }

        public void SetCurrentKey(int[] fields)
        {
            foreach (int num in fields)
            {
                this.CheckFieldNumber(num);
            }
            CFrontDll.SetCurrentKey(this.hTable, fields);
        }

        public void SetCurrentKey(string[] fields)
        {
            int[] numArray = new int[fields.Length];
            for (int i = 0; i < numArray.Length; i++)
            {
                numArray[i] = this.FieldNo(fields[i]);
            }
            CFrontDll.SetCurrentKey(this.hTable, numArray);
        }

        public void SetCurrentKey(TableKey key)
        {
            this.SetCurrentKey(key.fields);
        }

        public void SetFilter(int fieldNo, string filterExpr)
        {
            if (!this.varEOF)
            {
                CFrontDll.SetFilter(this.hTable, fieldNo, filterExpr);
                if (this.RecordCount == 0)
                {
                    this.varEOF = true;
                }
            }
            this.editMode = false;
        }

        public void SetFilter(string fieldName, string filterExpr)
        {
            if (!this.varEOF)
            {
                this.SetFilter(this.FieldNo(fieldName), filterExpr);
            }
        }

        public string TableName()
        {
            return CFrontDll.TableName(this.hTable);
        }

        public void Update()
        {
            if (!this.editMode)
            {
                throw new Exception("Para realizar una actualización es necesario acceder en modo edición");
            }
            if (this.isNew)
            {
                CFrontDll.InsertRec(this.hTable, this.hRec);
            }
            else
            {
                CFrontDll.ModifyRec(this.hTable, this.hRec);
            }
            this.editMode = false;
            this.isNew = false;
        }

        public bool BOF
        {
            get
            {
                return this.varBOF;
            }
        }

        public bool EOF
        {
            get
            {
                return this.varEOF;
            }
        }

        public short FieldCount
        {
            get
            {
                return CFrontDll.FieldCount(this.hTable);
            }
        }

        public string[] FieldNames
        {
            get
            {
                if (this.varFieldNames != null)
                {
                    return (string[]) this.varFieldNames.Clone();
                }
                int[] fieldNumbers = this.FieldNumbers;
                string[] strArray = new string[fieldNumbers.Length];
                for (int i = 0; i < strArray.Length; i++)
                {
                    strArray[i] = this.DllFieldName(fieldNumbers[i]);
                }
                this.varFieldNames = strArray;
                return (string[]) strArray.Clone();
            }
        }

        public int[] FieldNumbers
        {
            get
            {
                if (this.varFieldNumbers != null)
                {
                    return (int[]) this.varFieldNumbers.Clone();
                }
                int fieldNo = 0;
                int[] numArray = new int[this.FieldCount];
                int num2 = 0;
                for (fieldNo = CFrontDll.NextField(this.hTable, fieldNo); fieldNo > 0; fieldNo = CFrontDll.NextField(this.hTable, fieldNo))
                {
                    numArray[num2++] = fieldNo;
                }
                this.varFieldNumbers = numArray;
                return (int[]) numArray.Clone();
            }
        }

        public object this[int fieldNo]
        {
            get
            {
                this.CheckValidRecord();
                return CFrontDll.GetFieldValue(this.hRec, this.hTable, fieldNo);
            }
            set
            {
                if (!this.editMode)
                {
                    throw new Exception("Sólo es posible asignar valores a campos en modo edición. Usa Edit() o Addnew()");
                }
                this.CheckFieldNumber(fieldNo);
                CFrontDll.SetFieldValue(this.hRec, this.hTable, fieldNo, value);
            }
        }

        public object this[string fieldName]
        {
            get
            {
                int fieldNo = this.FieldNo(fieldName);
                return CFrontDll.GetFieldValue(this.hRec, this.hTable, fieldNo);
            }
            set
            {
                if (!this.editMode)
                {
                    throw new Exception("Sólo es posible asignar valores a campos en modo edición. Use Edit() or Addnew()");
                }
                int fieldNo = this.FieldNo(fieldName);
                CFrontDll.SetFieldValue(this.hRec, this.hTable, fieldNo, value);
            }
        }

        public virtual int RecordCount
        {
            get
            {
                return CFrontDll.RecordCount(this.hTable);
            }
        }
    }
}

