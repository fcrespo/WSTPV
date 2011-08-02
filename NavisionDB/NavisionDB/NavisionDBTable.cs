namespace NavisionDB
{
    using Microsoft.Navision.CFront;
    using System;
    using System.Data;

    public class NavisionDBTable
    {
        private object[,] actualizaciones;
        private string ClaveNavisionFormat;
        private int[] Claves;
        private string[] ClavesStr;
        private int[] Columnas;
        private string[] ColumnaStr;
        private NavisionDBConnection conexion;
        private TableFilters Filtros;
        private object[,] inserciones;
        private bool NoColumnas;
        private string[] nombreColumnas;
        private int numeroCampos;
        private int[] numeroColumnas;
        private bool Reves;
        private bool soloPrimeraFila;
        private int Tabla;
        private string TablaStr;
        private NavisionDBUser usuario;

        public NavisionDBTable(NavisionDBConnection Connection, NavisionDBUser User)
        {
            if (User == null)
            {
                throw new Exception("Error: User must be defined");
            }
            this.conexion = Connection;
            if (!this.ValidarUsuario(User))
            {
                throw new Exception("Error: User is a Nonvalid user");
            }
            this.Tabla = -1;
            this.TablaStr = "";
            this.usuario = User;
            this.Filtros = new TableFilters();
            this.NoColumnas = false;
            this.Reves = false;
            this.Claves = null;
            this.ClaveNavisionFormat = null;
            this.ClavesStr = null;
            this.Columnas = null;
            this.ColumnaStr = null;
            this.inserciones = null;
            this.actualizaciones = null;
            this.nombreColumnas = null;
            this.numeroColumnas = null;
            this.numeroCampos = 0;
            this.soloPrimeraFila = false;
        }

        public void AddColumn(int Column)
        {
            if (this.TableNo == -1)
            {
                throw new Exception("It's necessary to specify a table to add a column");
            }
            if (!this.valorAñadido(this.Columnas, Column))
            {
                this.insertarColumna(Column);
                this.insertarColumnaStr(this.nombreColumna(Column));
            }
        }

        public void AddColumn(string Column)
        {
            if (this.TableNo == -1)
            {
                throw new Exception("It's necessary to specify a table to add a column");
            }
            int valor = this.numeroColumna(Column);
            if (!this.valorAñadido(this.Columnas, valor))
            {
                this.insertarColumna(valor);
                this.insertarColumnaStr(Column);
            }
        }

        public void AddFilter(int fieldNo, string valor)
        {
            object[,] filtro = new object[,] { { fieldNo, valor } };
            this.Filtros.add(filtro);
        }

        public void AddFilter(string fieldName, string valor)
        {
            object[,] filtro = new object[,] { { this.numeroColumna(fieldName), valor } };
            this.Filtros.add(filtro);
        }

        private void añadirClaveFormatoNavision(string valor)
        {
            string fieldName = "";
            for (int i = 0; i < valor.Length; i++)
            {
                if (valor[i] == ',')
                {
                    this.Claves = this.añadirValor(this.Claves, this.numeroColumna(fieldName));
                    fieldName = "";
                }
                else
                {
                    fieldName = fieldName + valor[i];
                }
            }
            this.Claves = this.añadirValor(this.Claves, this.numeroColumna(fieldName));
        }

        private void AñadirRegistro(ref object[,] Tabla, int Column, object Value)
        {
            int num = 0;
            if (Tabla == null)
            {
                Tabla = new object[1, 2];
            }
            else
            {
                object[,] objArray = new object[Tabla.Length / 2, 2];
                objArray = Tabla;
                Tabla = new object[(Tabla.Length / 2) + 1, 2];
                for (int i = 0; i < (objArray.Length / 2); i++)
                {
                    Tabla[i, 0] = objArray[i, 0];
                    Tabla[i, 1] = objArray[i, 1];
                }
                num = objArray.Length / 2;
            }
            Tabla[num, 0] = Column;
            Tabla[num, 1] = Value;
        }

        private int[] añadirValor(int[] array, int valor)
        {
            if (array == null)
            {
                array = new int[] { valor };
                return array;
            }
            int[] numArray = new int[array.Length + 1];
            int index = 0;
            while (index < array.Length)
            {
                numArray[index] = array[index];
                index++;
            }
            numArray[index] = valor;
            return numArray;
        }

        public Type ColumnType(int Column)
        {
            NavisionFieldType code = NavisionFieldType.Code;
            CFrontRecordset recordset = this.OpenTable(this.TableNo);
            string typeName = recordset.FieldType(Column, ref code);
            recordset.FreeRec();
            recordset.Close();
            switch (typeName)
            {
                case "Code":
                    typeName = "System.String";
                    break;

                case "Decimal":
                    typeName = "System.Decimal";
                    break;

                case "Binary":
                    typeName = "System.String";
                    break;

                case "Blob":
                    typeName = "System.String";
                    break;

                case "Boolean":
                    typeName = "System.Boolean";
                    break;

                case "Date":
                    typeName = "System.DateTime";
                    break;

                case "DateFormula":
                    typeName = "System.String";
                    break;

                case "DateTime":
                    typeName = "System.DateTime";
                    break;

                case "Duration":
                    typeName = "System.String";
                    break;

                case "Guid":
                    typeName = "System.String";
                    break;

                case "Option":
                    typeName = "System.String";
                    break;

                case "RecordId":
                    typeName = "System.String";
                    break;

                case "Integer":
                    typeName = "System.Int32";
                    break;

                case "BigInteger":
                    typeName = "System.Int64";
                    break;

                case "TableFilter":
                    typeName = "System.String";
                    break;

                case "Time":
                    typeName = "System.DateTime";
                    break;

                case "Text":
                    typeName = "System.String";
                    break;

                default:
                    throw new Exception("Error: Unknown data type -> " + typeName);
            }
            return Type.GetType(typeName);
        }

        private string EliminarEspGuiPar(string Cadena)
        {
            string str = "";
            for (int i = 0; i < Cadena.Length; i++)
            {
                if ((((Cadena[i] != ' ') && (Cadena[i] != '(')) && ((Cadena[i] != ')') && (Cadena[i] != '-'))) && (((Cadena[i] != '%') && (Cadena[i] != '\x00ba')) && (Cadena[i] != '.')))
                {
                    str = str + Cadena[i];
                }
            }
            return str;
        }

        private string EliminarGuion(string Cadena)
        {
            string str = "";
            for (int i = 0; i < Cadena.Length; i++)
            {
                if ((Cadena[i] != '-') && (Cadena[i] != ' '))
                {
                    str = str + Cadena[i];
                }
            }
            return str;
        }

        public string FieldName(int fieldNo)
        {
            return this.nombreColumna(fieldNo);
        }

        public int FieldNo(string fieldName)
        {
            return this.numeroColumna(fieldName);
        }

        public DataSet GenerateStructure()
        {
            DataSet set = new DataSet();
            if (this.conexion == null)
            {
                throw new Exception("To invoke the method \"GenerateStructure\" it's necessary to establish a connection to Navision.");
            }
            if ((this.Tabla == -1) && (this.TablaStr == ""))
            {
                throw new Exception("To invoke the method \"GenerateStructure\" it's necessary to assign a value to the property TableNo or TableName.");
            }
            DataTable myDataTable = new DataTable(this.TableName);
            this.RellenarTabla(ref myDataTable);
            set.Tables.Add(myDataTable);
            set.AcceptChanges();
            return set;
        }

        public DataSet GenerateStructure(bool NavisionColumnName)
        {
            DataTable table;
            DataSet set = new DataSet();
            if (this.conexion == null)
            {
                throw new Exception("To invoke the method \"GenerateStructure\" it's necessary to establish a connection to Navision.");
            }
            if ((this.Tabla == -1) && (this.TablaStr == ""))
            {
                throw new Exception("To invoke the method \"GenerateStructure\" it's necessary to assign a value to the property TableNo or TableName.");
            }
            if (!NavisionColumnName)
            {
                table = new DataTable(this.EliminarEspGuiPar(this.TableName));
            }
            else
            {
                table = new DataTable(this.TableName);
            }
            this.RellenarTabla(ref table);
            if (!NavisionColumnName)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    table.Columns[i].ColumnName = this.EliminarEspGuiPar(table.Columns[i].ColumnName);
                }
                table.AcceptChanges();
            }
            set.Tables.Add(table);
            set.AcceptChanges();
            return set;
        }

        public void Insert(int Column, object Value)
        {
            this.AñadirRegistro(ref this.inserciones, Column, Value);
        }

        public void Insert(string Column, object Value)
        {
            if ((this.conexion != null) && ((this.Tabla != -1) || (this.TablaStr != "")))
            {
                this.Insert(this.numeroColumna(Column), Value);
            }
        }

        private void insertarColumna(int Columna)
        {
            if (this.Columnas != null)
            {
                int[] columnas = new int[this.Columnas.Length];
                columnas = this.Columnas;
                this.Columnas = new int[columnas.Length + 1];
                for (int i = 0; i < columnas.Length; i++)
                {
                    this.Columnas[i] = columnas[i];
                }
                this.Columnas[columnas.Length] = Columna;
            }
            else
            {
                this.Columnas = new int[] { Columna };
            }
        }

        private void insertarColumnaStr(string Columna)
        {
            if (this.ColumnaStr != null)
            {
                string[] columnaStr = new string[this.ColumnaStr.Length];
                columnaStr = this.ColumnaStr;
                this.ColumnaStr = new string[columnaStr.Length + 1];
                for (int i = 0; i < columnaStr.Length; i++)
                {
                    this.ColumnaStr[i] = columnaStr[i];
                }
                this.ColumnaStr[columnaStr.Length] = Columna;
            }
            else
            {
                this.ColumnaStr = new string[] { Columna };
            }
        }

        public void Modify(int Column, object Value)
        {
            this.AñadirRegistro(ref this.actualizaciones, Column, Value);
        }

        public void Modify(string Column, object Value)
        {
            if ((this.conexion != null) && ((this.Tabla != -1) || (this.TablaStr != "")))
            {
                this.Modify(this.numeroColumna(Column), Value);
            }
        }

        private string nombreColumna(int fieldNo)
        {
            for (int i = 0; i < this.numeroColumnas.Length; i++)
            {
                if (this.numeroColumnas[i] == fieldNo)
                {
                    return this.nombreColumnas[i];
                }
            }
            throw new Exception(string.Concat(new object[] { "The column ", fieldNo, " not belongs to table ", this.TableName }));
        }

        private int numeroColumna(string fieldName)
        {
            for (int i = 0; i < this.nombreColumnas.Length; i++)
            {
                if (this.nombreColumnas[i] == fieldName)
                {
                    return this.numeroColumnas[i];
                }
            }
            throw new Exception("The column " + fieldName + " not belongs to table " + this.TableName);
        }

        private CFrontRecordset OpenTable(int tableNo)
        {
            CFrontRecordset recordset;
            try
            {
                recordset = this.conexion.OpenTable(tableNo);
            }
            catch (Exception exception)
            {
                throw new Exception("Error: " + exception.Message);
            }
            return recordset;
        }

        private void RellenarTabla(ref DataTable myDataTable)
        {
            int[] numeroColumnas;
            string[] columnsName = null;
            int num;
            int numeroCampos;
            if (((this.Columnas == null) && (this.ColumnsName == null)) && !this.NoColumn)
            {
                numeroColumnas = this.numeroColumnas;
                numeroCampos = this.numeroCampos;
            }
            else if (this.Columns != null)
            {
                numeroColumnas = this.Columnas;
                numeroCampos = this.Columnas.Length;
            }
            else
            {
                numeroCampos = this.ColumnsName.Length;
                columnsName = this.ColumnsName;
                numeroColumnas = new int[numeroCampos];
                for (num = 0; num < numeroCampos; num++)
                {
                    numeroColumnas[num] = this.numeroColumna(columnsName[num]);
                }
            }
            this.conexion.SelectLatestVersion();
            CFrontRecordset recordset = this.conexion.OpenTable(this.Tabla);
            NavisionFieldType[] typeArray = new NavisionFieldType[numeroCampos];
            for (num = 0; num < numeroCampos; num++)
            {
                string str;
                string typeName = "";
                if (columnsName == null)
                {
                    str = this.nombreColumna(numeroColumnas[num]);
                }
                else
                {
                    str = columnsName[num];
                }
                switch (recordset.FieldType(numeroColumnas[num], ref typeArray[num]))
                {
                    case "Code":
                        typeName = "System.String";
                        break;

                    case "Decimal":
                        typeName = "System.Decimal";
                        break;

                    case "Binary":
                        typeName = "System.String";
                        break;

                    case "Blob":
                        typeName = "System.String";
                        break;

                    case "Boolean":
                        typeName = "System.Boolean";
                        break;

                    case "Date":
                        typeName = "System.DateTime";
                        break;

                    case "DateFormula":
                        typeName = "System.String";
                        break;

                    case "DateTime":
                        typeName = "System.DateTime";
                        break;

                    case "Duration":
                        typeName = "System.String";
                        break;

                    case "Guid":
                        typeName = "System.String";
                        break;

                    case "Option":
                        typeName = "System.String";
                        break;

                    case "RecordId":
                        typeName = "System.String";
                        break;

                    case "Integer":
                        typeName = "System.Int32";
                        break;

                    case "BigInteger":
                        typeName = "System.Int64";
                        break;

                    case "TableFilter":
                        typeName = "System.String";
                        break;

                    case "Time":
                        typeName = "System.DateTime";
                        break;

                    case "Text":
                        typeName = "System.String";
                        break;

                    default:
                        throw new Exception("Error: Unknown data type -> " + recordset.FieldType(numeroColumnas[num], ref typeArray[num]).ToString());
                }
                DataColumn column = new DataColumn(str, Type.GetType(typeName));
                myDataTable.Columns.Add(column);
            }
            myDataTable.AcceptChanges();
            recordset.FreeRec();
            recordset.Close();
        }

        public void Reset()
        {
            this.Tabla = -1;
            this.TablaStr = "";
            this.Filtros = new TableFilters();
            this.NoColumnas = false;
            this.Reves = false;
            this.Claves = null;
            this.ClaveNavisionFormat = null;
            this.ClavesStr = null;
            this.Columnas = null;
            this.ColumnaStr = null;
            this.inserciones = null;
            this.actualizaciones = null;
            this.nombreColumnas = null;
            this.numeroColumnas = null;
            this.numeroCampos = 0;
            this.soloPrimeraFila = false;
        }

        private bool ValidarUsuario(NavisionDBUser usuarioL)
        {
            if (this.conexion == null)
            {
                throw new Exception("It's necessary to establish a connection to Navision to assign a value to the property TableName");
            }
            if (usuarioL == null)
            {
                return false;
            }
            if (usuarioL.UserId == "")
            {
                return false;
            }
            if (usuarioL.UserId != "anonymous")
            {
                this.conexion.SelectLatestVersion();
                CFrontRecordset recordset = this.conexion.OpenTable("Mobile Users");
                recordset.SetFilter("User ID", usuarioL.UserId);
                recordset.SetFilter("Password", usuarioL.Password);
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

        private bool valorAñadido(int[] array, int valor)
        {
            if (array != null)
            {
                for (int i = 0; i < array.Length; i++)
                {
                    if (array[i] == valor)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public int[] Columns
        {
            get
            {
                return this.Columnas;
            }
        }

        public string[] ColumnsName
        {
            get
            {
                return this.ColumnaStr;
            }
        }

        public NavisionDBConnection ConnectionDB
        {
            get
            {
                return this.conexion;
            }
        }

        public int FieldCount
        {
            get
            {
                return this.numeroCampos;
            }
        }

        public string[] FieldNames
        {
            get
            {
                return this.nombreColumnas;
            }
        }

        public int[] FieldNumbers
        {
            get
            {
                return this.numeroColumnas;
            }
        }

        public object[,] Filters
        {
            get
            {
                return this.Filtros.get();
            }
        }

        public bool firstRow
        {
            get
            {
                return this.soloPrimeraFila;
            }
            set
            {
                this.soloPrimeraFila = value;
            }
        }

        public object[,] Inserts
        {
            get
            {
                return this.inserciones;
            }
        }

        public int[] Key
        {
            get
            {
                return this.Claves;
            }
            set
            {
                this.Claves = value;
            }
        }

        public string[] KeyByName
        {
            get
            {
                return this.ClavesStr;
            }
            set
            {
                this.ClavesStr = value;
                if (this.ClavesStr != null)
                {
                    int[] numArray = new int[this.ClavesStr.Length];
                    for (int i = 0; i < this.ClavesStr.Length; i++)
                    {
                        numArray[i] = this.numeroColumna(this.ClavesStr[i]);
                    }
                    this.Claves = numArray;
                }
                else
                {
                    this.Claves = null;
                }
            }
        }

        public string KeyInNavisionFormat
        {
            get
            {
                return this.ClaveNavisionFormat;
            }
            set
            {
                this.ClaveNavisionFormat = value;
                this.añadirClaveFormatoNavision(value);
            }
        }

        public bool NoColumn
        {
            get
            {
                return this.NoColumnas;
            }
            set
            {
                this.NoColumnas = value;
            }
        }

        public bool Reverse
        {
            get
            {
                return this.Reves;
            }
            set
            {
                this.Reves = value;
            }
        }

        public string TableName
        {
            get
            {
                return this.TablaStr;
            }
            set
            {
                if (this.conexion == null)
                {
                    throw new Exception("To assign a value to the property TableName it's necessary to establish a connection to Navision.");
                }
                this.TablaStr = value;
                if (this.Tabla == -1)
                {
                    this.Tabla = this.conexion.TableNo(this.TablaStr);
                    CFrontRecordset recordset = this.OpenTable(this.Tabla);
                    this.nombreColumnas = recordset.FieldNames;
                    this.numeroColumnas = recordset.FieldNumbers;
                    this.numeroCampos = recordset.FieldCount;
                    recordset.FreeRec();
                    recordset.Close();
                }
            }
        }

        public int TableNo
        {
            get
            {
                return this.Tabla;
            }
            set
            {
                if (this.conexion == null)
                {
                    throw new Exception("To assign a value to the property TableNo it's necessary to establish a connection to Navision.");
                }
                this.Tabla = value;
                if (this.TablaStr == "")
                {
                    CFrontRecordset recordset = this.OpenTable(this.Tabla);
                    this.TablaStr = recordset.TableName();
                    this.nombreColumnas = recordset.FieldNames;
                    this.numeroColumnas = recordset.FieldNumbers;
                    this.numeroCampos = recordset.FieldCount;
                    recordset.FreeRec();
                    recordset.Close();
                }
            }
        }

        public object[,] Updates
        {
            get
            {
                return this.actualizaciones;
            }
        }

        public NavisionDBUser User
        {
            get
            {
                return this.usuario;
            }
        }
    }
}

