namespace NavisionDB
{
    using System;
    using System.Data;
    using System.Reflection;

    public class NavisionDBDataReader : MarshalByRefObject
    {
        private bool Cerrado;
        private int NumColumna;
        private int NumColumnas;
        private int NumFila;
        private int RegAfectados;
        private DataTable Resultados;

        public NavisionDBDataReader()
        {
            this.Cerrado = false;
            this.NumColumnas = 0;
            this.RegAfectados = 0;
        }

        public NavisionDBDataReader(DataTable Tabla)
        {
            this.Resultados = Tabla;
            this.NumFila = 0;
            this.NumColumna = 0;
            this.NumColumnas = this.Resultados.Columns.Count;
            this.RegAfectados = this.Resultados.Rows.Count;
            this.Cerrado = false;
        }

        public void Close()
        {
            this.Cerrado = true;
        }

        public bool GetBoolean(int NumColumn)
        {
            return Convert.ToBoolean(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public byte GetByte(int NumColumn)
        {
            return Convert.ToByte(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public char GetChar(int NumColumn)
        {
            return Convert.ToChar(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public DateTime GetDataTime(int NumColumn)
        {
            return Convert.ToDateTime(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public string GetDataTypeName(int NumColumn)
        {
            return Convert.ToString(this.Resultados.Rows[this.NumFila][NumColumn].GetType());
        }

        public decimal GetDecimal(int NumColumn)
        {
            return Convert.ToDecimal(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public double GetDouble(int NumColumn)
        {
            return Convert.ToDouble(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public string GetGuid(int NumColumn)
        {
            return Convert.ToString(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public short GetInt16(int NumColumn)
        {
            return Convert.ToInt16(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public int GetInt32(int NumColumn)
        {
            return Convert.ToInt32(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public long GetInt64(int NumColumn)
        {
            return Convert.ToInt64(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public string GetName(int NumColumn)
        {
            return this.Resultados.Columns[NumColumn].ColumnName;
        }

        public string GetString(int NumColumn)
        {
            return Convert.ToString(this.Resultados.Rows[this.NumFila][NumColumn]);
        }

        public object GetValue(int NumColumn)
        {
            return this.Resultados.Rows[this.NumFila][NumColumn];
        }

        public object[] GetValues()
        {
            object[] objArray = new object[this.Resultados.Columns.Count];
            for (int i = 0; i < objArray.Length; i++)
            {
                objArray[i] = this.Resultados.Rows[this.NumFila][i];
            }
            return objArray;
        }

        public bool IsDBNull()
        {
            return (this.Resultados.Rows[this.NumFila][this.NumColumna] == DBNull.Value);
        }

        public bool IsDBNull(int NumColumn)
        {
            return (this.Resultados.Rows[this.NumFila][NumColumn] == DBNull.Value);
        }

        public bool NextResult()
        {
            if (this.NumColumna == (this.Resultados.Columns.Count - 1))
            {
                if (this.NumFila == (this.Resultados.Rows.Count - 1))
                {
                    return false;
                }
                this.NumFila++;
                this.NumColumna = 0;
            }
            else
            {
                this.NumColumna++;
            }
            return true;
        }

        public bool Read()
        {
            if (this.NumFila == (this.Resultados.Rows.Count - 1))
            {
                return false;
            }
            this.NumFila++;
            return true;
        }

        public int FieldCount
        {
            get
            {
                return this.NumColumnas;
            }
        }

        public bool IsClosed
        {
            get
            {
                return this.Cerrado;
            }
        }

        public object this[int Column]
        {
            get
            {
                return this.Resultados.Rows[this.NumFila][Column];
            }
        }

        public int RecordsAffected
        {
            get
            {
                return this.RegAfectados;
            }
        }
    }
}

