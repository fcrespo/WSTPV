namespace NavisionDB
{
    using Microsoft.Navision.CFront;
    using System;
    using System.Data;

    public class NavisionDBCommand
    {
        private NavisionDBConnection conexion;
        private object[] Parametros;
        private NavisionDBTable Tabla;

        public NavisionDBCommand(NavisionDBConnection Connection) 
        {
            this.conexion = Connection;
        }

        public void CreateParameter(object[] Parameters)
        {
            this.Parametros = new object[5];
            for (int i = 0; i < this.Parametros.Length; i++)
            {
                this.Parametros[i] = Parameters[i];
            }
        }

        public double ExecuteCalcSums(bool LockTable, int FieldNo)
        {
            double num = 0.0;
            CFrontRecordset recordset = this.PrepararConsultaCalcSums(LockTable);
            if (!recordset.EOF)
            {
                num = Convert.ToDouble(recordset[FieldNo]);
            }
            recordset.FreeRec();
            recordset.Close();
            return num;
        }

        public int ExecuteNonQuery(bool LockTable)
        {
            int recordCount = 0;
            CFrontRecordset recordset = this.PrepararConsulta(LockTable);
            recordCount = recordset.RecordCount;
            recordset.FreeRec();
            recordset.Close();
            return recordCount;
        }

        public NavisionDBDataReader ExecuteReader(bool LockTable)
        {
            NavisionDBDataReader reader;
            DataRow row;
            int[] fieldNumbers;
            int num;
            int fieldCount;
            CFrontRecordset recordset = this.PrepararConsulta(LockTable);
            if (recordset.EOF)
            {
                reader = new NavisionDBDataReader();
                goto Label_03B0;
            }
            DataTable tabla = new DataTable(this.Tabla.TableName);
            string[] columnsName = null;
            if (((this.Tabla.Columns == null) && (this.Tabla.ColumnsName == null)) && !this.Tabla.NoColumn)
            {
                fieldNumbers = this.Tabla.FieldNumbers;
                fieldCount = this.Tabla.FieldCount;
            }
            else
            {
                fieldCount = this.Tabla.Columns.Length;
                fieldNumbers = this.Tabla.Columns;
                columnsName = this.Tabla.ColumnsName;
            }
            NavisionFieldType[] typeArray = new NavisionFieldType[fieldCount];
            for (num = 0; num < fieldCount; num++)
            {
                string str;
                string typeName = "";
                if (columnsName == null)
                {
                    str = recordset.FieldName(fieldNumbers[num]);
                }
                else
                {
                    str = columnsName[num];
                }
                switch (recordset.FieldType(fieldNumbers[num], ref typeArray[num]))
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
                        throw new Exception("Error: Unknown data type -> " + recordset.FieldType(fieldNumbers[num], ref typeArray[num]).ToString());
                }
                DataColumn column = new DataColumn(str, Type.GetType(typeName));
                tabla.Columns.Add(column);
            }
        Label_0335:
            row = tabla.NewRow();
            for (num = 0; num < fieldCount; num++)
            {
                row[num] = recordset[fieldNumbers[num]];
            }
            tabla.Rows.Add(row);
            if (!this.Tabla.firstRow)
            {
                if (this.Tabla.Reverse)
                {
                    if (recordset.MovePrevious())
                    {
                        goto Label_0335;
                    }
                }
                else if (recordset.MoveNext())
                {
                    goto Label_0335;
                }
            }
            tabla.AcceptChanges();
            reader = new NavisionDBDataReader(tabla);
        Label_03B0:
            recordset.FreeRec();
            recordset.Close();
            return reader;
        }

        public object ExecuteScalar(bool LockTable)
        {
            object obj2 = null;
            CFrontRecordset recordset = this.PrepararConsulta(LockTable);
            if (!recordset.EOF)
            {
                int[] fieldNumbers = this.Tabla.FieldNumbers;
                obj2 = recordset[fieldNumbers[0]];
            }
            recordset.FreeRec();
            recordset.Close();
            return obj2;
        }

        private CFrontRecordset PrepararConsulta(bool Bloqueo)
        {
            CFrontRecordset recordset;
            if (this.Tabla == null)
            {
                throw new Exception("It's necessary to assign value to \"ConnectionDB\" property.");
            }
            if ((this.Tabla.TableNo == -1) && (this.Tabla.TableName == ""))
            {
                throw new Exception("It's necessary to assign value to \"Table\" property.");
            }
            this.conexion.SelectLatestVersion();
            if (this.Tabla.TableNo != -1)
            {
                recordset = this.Tabla.ConnectionDB.OpenTable(this.Tabla.TableNo);
            }
            else
            {
                recordset = this.Tabla.ConnectionDB.OpenTable(this.Tabla.TableName);
            }
            if (Bloqueo)
            {
                recordset.LockTable();
            }
            if (this.Tabla.Key != null)
            {
                recordset.SetCurrentKey(this.Tabla.Key);
            }
            object[,] filters = this.Tabla.Filters;
            if (filters != null)
            {
                for (int i = 0; i < (filters.Length / 2); i++)
                {
                    recordset.SetFilter(Convert.ToInt32(filters[i, 0]), Convert.ToString(filters[i, 1]));
                }
            }
            if (this.Tabla.Reverse)
            {
                recordset.MoveLast();
                return recordset;
            }
            recordset.MoveFirst();
            return recordset;
        }

        private CFrontRecordset PrepararConsultaCalcSums(bool Bloqueo)
        {
            CFrontRecordset recordset;
            int[] fieldNumbers;
            int fieldCount;
            if (this.Tabla == null)
            {
                throw new Exception("It's necessary to assign value to \"ConnectionDB\" property.");
            }
            if ((this.Tabla.TableNo == -1) && (this.Tabla.TableName == ""))
            {
                throw new Exception("It's necessary to assign value to \"Table\" property.");
            }
            this.conexion.SelectLatestVersion();
            if (this.Tabla.TableNo != -1)
            {
                recordset = this.Tabla.ConnectionDB.OpenTable(this.Tabla.TableNo);
            }
            else
            {
                recordset = this.Tabla.ConnectionDB.OpenTable(this.Tabla.TableName);
            }
            if (Bloqueo)
            {
                recordset.LockTable();
            }
            if (this.Tabla.Key != null)
            {
                recordset.SetCurrentKey(this.Tabla.Key);
            }
            object[,] filters = this.Tabla.Filters;
            if (filters != null)
            {
                for (int i = 0; i < (filters.Length / 2); i++)
                {
                    recordset.SetFilter(Convert.ToInt32(filters[i, 0]), Convert.ToString(filters[i, 1]));
                }
            }
            string[] columnsName = null;
            if (((this.Tabla.Columns == null) && (this.Tabla.ColumnsName == null)) && !this.Tabla.NoColumn)
            {
                fieldNumbers = this.Tabla.FieldNumbers;
                fieldCount = this.Tabla.FieldCount;
            }
            else if (this.Tabla.Columns != null)
            {
                fieldNumbers = this.Tabla.Columns;
                fieldCount = this.Tabla.Columns.Length;
            }
            else
            {
                fieldCount = this.Tabla.ColumnsName.Length;
                columnsName = this.Tabla.ColumnsName;
                fieldNumbers = new int[fieldCount];
                for (int j = 0; j < fieldCount; j++)
                {
                    fieldNumbers[j] = this.Tabla.FieldNo(columnsName[j]);
                }
            }
            recordset.MoveFirst();
            recordset.CalcSums(fieldNumbers);
            return recordset;
        }

        public NavisionDBConnection ConnectionDB
        {
            get
            {
                return this.conexion;
            }
        }

        public object[] Parameters
        {
            get
            {
                this.Parametros = new object[] { this.conexion, this.Tabla };
                return this.Parametros;
            }
        }

        public NavisionDBTable Table
        {
            get
            {
                return this.Tabla;
            }
            set
            {
                this.Tabla = value;
            }
        }
    }
}

