namespace NavisionDB
{
    using Microsoft.Navision.CFront;
    using System;
    using System.ComponentModel;
    using System.Data;

    public class NavisionDBAdapter : Component
    {
        private DataTable Actualizaciones = new DataTable("Actualizaciones");
        private DataTable Eliminaciones = new DataTable("Eliminaciones");
        private DataTable Inserciones = new DataTable("Inserciones");
        private NavisionDBRelation Relaciones = new NavisionDBRelation();
        private NavisionDBTable[] Tablas;
        private const string VALOR_FILTRO_BLANCO = " ";

        private void ActualizaResultJoin(ref NavisionDBRelation.ResultJoin[] resJoin, ref int indexP, ref int indexH, string TPadre, string THijo)
        {
            if (indexH != -1)
            {
                resJoin[indexH].AniadeTablaJoin(TPadre);
            }
            if (indexP != -1)
            {
                resJoin[indexP].AniadeTablaJoin(THijo);
            }
            if ((indexH == -1) && (indexP == -1))
            {
                if (resJoin == null)
                {
                    resJoin = new NavisionDBRelation.ResultJoin[1];
                    resJoin[0].tNames = new string[] { TPadre, THijo };
                    indexP = 0;
                    indexH = 0;
                }
                else
                {
                    NavisionDBRelation.ResultJoin[] joinArray = new NavisionDBRelation.ResultJoin[resJoin.Length + 1];
                    for (int i = 0; i < resJoin.Length; i++)
                    {
                        joinArray[i] = resJoin[i];
                        joinArray[i].tNames = new string[resJoin[i].tNames.Length];
                        for (int j = 0; j < joinArray[i].tNames.Length; j++)
                        {
                            joinArray[i].tNames[j] = resJoin[i].tNames[j];
                        }
                    }
                    joinArray[joinArray.Length - 1].tNames = new string[] { TPadre, THijo };
                    resJoin = joinArray;
                    indexP = joinArray.Length - 1;
                    indexH = indexP;
                }
            }
        }

        private void ActualizarRegistros(DataTable registros)
        {
            try
            {
                int num;
                if (this.Tables[0] == null)
                {
                    throw new Exception("It's necessary to assign value to \"ConnectionDB\" property.");
                }
                if (this.Tables[0].TableNo == -1)
                {
                    throw new Exception("It's necessary to assign value to \"Tables\" property.");
                }
                this.Tables[0].ConnectionDB.SelectLatestVersion();
                CFrontRecordset recordset = this.Tables[0].ConnectionDB.OpenTable(this.Tables[0].TableNo);
                if (this.Tables[0].Key != null)
                {
                    recordset.SetCurrentKey(this.Tables[0].Key);
                }
                object[,] filters = this.Tables[0].Filters;
                if (filters != null)
                {
                    for (num = 0; num < (filters.Length / 2); num++)
                    {
                        recordset.SetFilter(Convert.ToInt32(filters[num, 0]), Convert.ToString(filters[num, 1]));
                    }
                }
                recordset.MoveFirst();
                if (!recordset.EOF)
                {
                    recordset.LockTable();
                    int[] fieldNumbers = this.Tables[0].FieldNumbers;
                    for (num = 0; num < registros.Rows.Count; num++)
                    {
                        recordset.Edit();
                        for (int i = 0; i < registros.Columns.Count; i++)
                        {
                            recordset[this.Tables[0].FieldNo(registros.Columns[i].ColumnName)] = registros.Rows[num][i];
                        }
                        recordset.Update();
                    }
                }
                recordset.FreeRec();
                recordset.Close();
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message);
            }
        }

        public void AddRelation(object ParentTable, object ParentRelation, object ChildTable, object ChildRelation, JoinType TypeOfJoin, bool Distinct)
        {
            NavisionDBRelation.Relation relation;
            if (!this.ValidarUsuario())
            {
                throw new Exception("Nonvalid user.");
            }
            int index = 0;
            int num2 = 0;
            string[,] strArray = new string[1, 6];
            if (!(ParentTable is string))
            {
                try
                {
                    int num3 = Convert.ToInt32(ParentTable);
                    index = 0;
                    while (index < this.Tablas.Length)
                    {
                        if (this.Tablas[index].TableNo == num3)
                        {
                            break;
                        }
                        index++;
                    }
                    if (index == this.Tablas.Length)
                    {
                        throw new Exception("In order to establish a relation between tables must be necessary that the implied tables have been assigned in the Tables property");
                    }
                    strArray[0, 0] = this.Tablas[index].TableName;
                    goto Label_00DB;
                }
                catch
                {
                    throw new Exception("Parameter ParentTable must be of type 'integer' or 'string'");
                }
            }
            string str = ParentTable.ToString();
            index = 0;
            while (index < this.Tablas.Length)
            {
                if (this.Tablas[index].TableName == str)
                {
                    break;
                }
                index++;
            }
            if (index == this.Tablas.Length)
            {
                throw new Exception("In order to establish a relation between tables must be necessary that the implied tables have been assigned in the Tables property");
            }
            strArray[0, 0] = str;
        Label_00DB:
            if (!(ParentRelation is string))
            {
                try
                {
                    int fieldNo = Convert.ToInt32(ParentRelation);
                    strArray[0, 1] = this.Tablas[index].FieldName(fieldNo);
                    goto Label_011E;
                }
                catch
                {
                    throw new Exception("Parameter ParentRelation must be of type 'integer' or 'string'");
                }
            }
            strArray[0, 1] = ParentRelation.ToString();
        Label_011E:
            if (!(ChildTable is string))
            {
                try
                {
                    int num5 = Convert.ToInt32(ChildTable);
                    num2 = 0;
                    while (num2 < this.Tablas.Length)
                    {
                        if (this.Tablas[num2].TableNo == num5)
                        {
                            break;
                        }
                        num2++;
                    }
                    if (num2 == this.Tablas.Length)
                    {
                        throw new Exception("In order to establish a relation between tables must be necessary that the implied tables have been assigned in the Tables property");
                    }
                    strArray[0, 2] = this.Tablas[num2].TableName;
                    goto Label_01DC;
                }
                catch
                {
                    throw new Exception("Parameter ChildTable must be of type 'integer' or 'string'");
                }
            }
            string str2 = ChildTable.ToString();
            num2 = 0;
            while (num2 < this.Tablas.Length)
            {
                if (this.Tablas[num2].TableName == str2)
                {
                    break;
                }
                num2++;
            }
            if (num2 == this.Tablas.Length)
            {
                throw new Exception("In order to establish a relation between tables must be necessary that the implied tables have been assigned in the Tables property");
            }
            strArray[0, 2] = str2;
        Label_01DC:
            if (!(ChildRelation is string))
            {
                try
                {
                    int num6 = Convert.ToInt32(ChildRelation);
                    strArray[0, 3] = this.Tablas[num2].FieldName(num6);
                    goto Label_0222;
                }
                catch
                {
                    throw new Exception("Parameter ChildTable must be of type 'integer' or 'string'");
                }
            }
            strArray[0, 3] = ChildRelation.ToString();
        Label_0222:
            strArray[0, 4] = Convert.ToString(TypeOfJoin);
            strArray[0, 5] = Convert.ToString(Distinct);
            relation.Parent_T = strArray[0, 0];
            relation.Child_T = strArray[0, 2];
            relation.RelationType = TypeOfJoin;
            relation.distinct = Distinct;
            relation.RelationCol = new NavisionDBRelation.RelationColumns_t[1];
            relation.RelationCol[0].PColumn = strArray[0, 1];
            relation.RelationCol[0].ChColumn = strArray[0, 3];
            this.Relaciones.add(relation);
        }

        public void AddTable(NavisionDBTable Table)
        {
            int num;
            int index = 0;
            if (this.Tablas == null)
            {
                this.Tablas = new NavisionDBTable[1];
            }
            else
            {
                NavisionDBTable[] tablas = new NavisionDBTable[this.Tablas.Length];
                tablas = this.Tablas;
                this.Tablas = new NavisionDBTable[this.Tablas.Length + 1];
                for (num = 0; num < tablas.Length; num++)
                {
                    this.Tablas[num] = new NavisionDBTable(tablas[num].ConnectionDB, tablas[num].User);
                    this.Tablas[num] = tablas[num];
                }
                index = tablas.Length;
            }
            this.Tablas[index] = new NavisionDBTable(Table.ConnectionDB, Table.User);
            this.Tablas[index].TableName = Table.TableName;
            this.Tablas[index].TableNo = Table.TableNo;
            if (Table.Filters != null)
            {
                object[,] filters = Table.Filters;
                for (num = 0; num < (filters.Length / 2); num++)
                {
                    this.Tablas[index].AddFilter(Convert.ToInt32(filters[num, 0]), Convert.ToString(filters[num, 1]));
                }
            }
            this.Tablas[index].NoColumn = Table.NoColumn;
            this.Tablas[index].Reverse = Table.Reverse;
            this.Tablas[index].Key = Table.Key;
            if (Table.Columns != null)
            {
                int[] columns = Table.Columns;
                for (num = 0; num < columns.Length; num++)
                {
                    this.Tablas[index].AddColumn(columns[num]);
                }
            }
            if (Table.Inserts != null)
            {
                object[,] inserts = Table.Inserts;
                for (num = 0; num < (inserts.Length / 2); num++)
                {
                    this.Tablas[index].Insert(Convert.ToInt32(inserts[num, 0]), inserts[num, 1]);
                }
            }
            if (Table.Updates != null)
            {
                object[,] updates = Table.Updates;
                for (num = 0; num < (updates.Length / 2); num++)
                {
                    this.Tablas[index].Modify(Convert.ToInt32(updates[num, 0]), updates[num, 1]);
                }
            }
            this.Tablas[index].firstRow = Table.firstRow;
        }

        private void AñadirCol(ref string[,] TablaColumnas, string Tabla, string Columna)
        {
            int num = 0;
            if (TablaColumnas[0, 0] != null)
            {
                string[,] strArray = new string[TablaColumnas.Length / 2, 2];
                strArray = TablaColumnas;
                TablaColumnas = new string[(TablaColumnas.Length / 2) + 1, 2];
                for (int i = 0; i < (strArray.Length / 2); i++)
                {
                    TablaColumnas[i, 0] = strArray[i, 0];
                    TablaColumnas[i, 1] = strArray[i, 1];
                }
                num = strArray.Length / 2;
            }
            TablaColumnas[num, 0] = Tabla;
            TablaColumnas[num, 1] = Columna;
        }

        private void AñadirColumna(ref DataTable Tabla, string NombreColumna, Type Tipo)
        {
            DataColumn column = new DataColumn(NombreColumna, Tipo);
            Tabla.Columns.Add(column);
        }

        private void AñadirFila(ref DataTable Destino, DataSet Origen)
        {
            try
            {
                for (int i = 0; i < Origen.Tables[0].Rows.Count; i++)
                {
                    Destino.ImportRow(Origen.Tables[0].Rows[i]);
                }
                Destino.AcceptChanges();
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message);
            }
        }

        private bool AñadirRelacionadas(ref string[][] Raux, DataTable[] TAux, ref int TRel, ref int TResRel)
        {
            string[] strArray;
            if (Raux.Length == 0)
            {
                strArray = new string[2];
                Raux = new string[1][];
                strArray[0] = TAux[0].TableName;
                strArray[1] = TAux[1].TableName;
                Raux[0] = strArray;
                TResRel = 0;
                return false;
            }
            int index = 0;
            while (index < Raux.Length)
            {
                int num2 = 0;
                while (num2 < Raux[index].Length)
                {
                    if (Raux[index][num2] == TAux[0].TableName)
                    {
                        TRel = 1;
                        break;
                    }
                    if (Raux[index][num2] == TAux[1].TableName)
                    {
                        TRel = 0;
                        break;
                    }
                    num2++;
                }
                if (num2 != Raux[index].Length)
                {
                    break;
                }
                index++;
            }
            if (index != Raux.Length)
            {
                strArray = new string[Raux[index].Length];
                strArray = Raux[index];
                Raux[index] = new string[Raux[index].Length + 1];
                for (int i = 0; i < strArray.Length; i++)
                {
                    Raux[index][i] = strArray[i];
                }
                if (TRel == 0)
                {
                    Raux[index][strArray.Length] = TAux[0].TableName;
                }
                else
                {
                    Raux[index][strArray.Length] = TAux[1].TableName;
                }
                TResRel = index;
                return true;
            }
            strArray = new string[] { TAux[0].TableName, TAux[1].TableName };
            string[][] strArray2 = new string[Raux.Length][];
            strArray2 = Raux;
            Raux = new string[Raux.Length + 1][];
            for (index = 0; index < strArray2.Length; index++)
            {
                Raux[index] = strArray2[index];
            }
            Raux[strArray2.Length] = strArray;
            return false;
        }

        private void BorrarColAñadidaJoin(string[,] añadidas, DataTable tabla)
        {
            if (añadidas != null)
            {
                string name = "";
                int num = añadidas.Length / 2;
                for (int i = 0; i < num; i++)
                {
                    name = añadidas[i, 0] + "." + añadidas[i, 1];
                    if (tabla.Columns.Contains(name))
                    {
                        tabla.Columns.Remove(name);
                        tabla.AcceptChanges();
                    }
                }
            }
        }

        private void CopiarDBTable(NavisionDBTable t_origen, ref NavisionDBTable t_destino)
        {
            int num;
            t_destino.TableName = t_origen.TableName;
            t_destino.TableNo = t_origen.TableNo;
            t_destino.Key = t_origen.Key;
            t_destino.Reverse = t_origen.Reverse;
            if (t_origen.Filters != null)
            {
                object[,] filters = t_origen.Filters;
                for (num = 0; num < (filters.Length / 2); num++)
                {
                    t_destino.AddFilter(Convert.ToInt32(filters[num, 0]), Convert.ToString(filters[num, 1]));
                }
            }
            if (t_origen.Columns != null)
            {
                int[] columns = t_origen.Columns;
                for (num = 0; num < columns.Length; num++)
                {
                    t_destino.AddColumn(columns[num]);
                }
            }
            if (t_origen.ColumnsName != null)
            {
                string[] columnsName = t_origen.ColumnsName;
                for (num = 0; num < columnsName.Length; num++)
                {
                    t_destino.AddColumn(columnsName[num]);
                }
            }
            t_destino.NoColumn = t_origen.NoColumn;
            if (t_origen.Inserts != null)
            {
                object[,] inserts = t_origen.Inserts;
                for (num = 0; num < (inserts.Length / 2); num++)
                {
                    t_destino.Insert(Convert.ToInt32(inserts[num, 0]), inserts[num, 1]);
                }
            }
            if (t_origen.Updates != null)
            {
                object[,] updates = t_origen.Updates;
                for (num = 0; num < (updates.Length / 2); num++)
                {
                    t_destino.Modify(Convert.ToInt32(updates[num, 0]), updates[num, 1]);
                }
            }
        }

        private string EliminarEspGuiPar(string Cadena)
        {
            string str = "";
            for (int i = 0; i < Cadena.Length; i++)
            {
                if ((((Cadena[i] != ' ') && (Cadena[i] != '(')) && ((Cadena[i] != ')') && (Cadena[i] != '-'))) && (((Cadena[i] != '%') && (Cadena[i] != '.')) && (Cadena[i] != '\x00ba')))
                {
                    str = str + Cadena[i];
                }
            }
            return str;
        }

        private string EliminarGuionYPunto(string Cadena)
        {
            string str = "";
            for (int i = 0; i < Cadena.Length; i++)
            {
                if (((Cadena[i] != '-') && (Cadena[i] != ' ')) && ((Cadena[i] != '.') && (Cadena[i] != '\x00ba')))
                {
                    str = str + Cadena[i];
                }
            }
            return str;
        }

        private void EliminarRegistros(DataTable registros)
        {
            try
            {
                int num;
                if (this.Tables[0] == null)
                {
                    throw new Exception("It's necessary to assign value to \"ConnectionDB\" property.");
                }
                if (this.Tables[0].TableNo == -1)
                {
                    throw new Exception("It's necessary to assign value to \"Tables\" property.");
                }
                this.Tables[0].ConnectionDB.SelectLatestVersion();
                CFrontRecordset recordset = this.Tables[0].ConnectionDB.OpenTable(this.Tables[0].TableNo);
                if (this.Tables[0].Key != null)
                {
                    recordset.SetCurrentKey(this.Tables[0].Key);
                }
                object[,] filters = this.Tables[0].Filters;
                if (filters != null)
                {
                    for (num = 0; num < (filters.Length / 2); num++)
                    {
                        recordset.SetFilter(Convert.ToInt32(filters[num, 0]), Convert.ToString(filters[num, 1]));
                    }
                }
                recordset.MoveFirst();
                if (!recordset.EOF)
                {
                    recordset.LockTable();
                    for (num = 0; num < registros.Rows.Count; num++)
                    {
                        int num2 = 0;
                        while (num2 < registros.Columns.Count)
                        {
                            if (recordset[this.Tables[0].FieldNo(registros.Columns[num2].ColumnName)].ToString().ToUpper() != registros.Rows[num][num2].ToString().ToUpper())
                            {
                                num2 = 0;
                                if (recordset.MoveStep(1) != 1)
                                {
                                    break;
                                }
                            }
                            num2++;
                        }
                        if (num2 == registros.Columns.Count)
                        {
                            recordset.Delete();
                        }
                        recordset.MoveFirst();
                    }
                }
                recordset.FreeRec();
                recordset.Close();
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message);
            }
        }

        private bool ExisteCampoEnVista(int num, int Columna)
        {
            int num2;
            int[] columns = null;
            string[] columnsName = null;
            if (this.Tablas[num].Columns != null)
            {
                columns = this.Tablas[num].Columns;
            }
            else if (this.Tablas[num].ColumnsName != null)
            {
                columnsName = this.Tablas[num].ColumnsName;
                columns = new int[this.Tablas[num].ColumnsName.Length];
                for (num2 = 0; num2 < this.Tablas[num].ColumnsName.Length; num2++)
                {
                    columns[num2] = this.Tablas[num].FieldNo(columnsName[num2]);
                }
            }
            if (!this.Tablas[num].NoColumn)
            {
                if (columns == null)
                {
                    return true;
                }
                for (num2 = 0; num2 < columns.Length; num2++)
                {
                    if (columns[num2] == Columna)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public void Fill(ref DataSet ds, bool NavisionColumnName)
        {
            NavisionDBRelation.ResultJoin[] resJoinAux = null;
            DataRelation[] relationArray = null;
            int num;
            string[,] tablaColumnas = new string[1, 2];
            if (this.Tablas.Length == 0)
            {
                throw new Exception("It's necessary to add at least 1 table to the \"Tables\" property");
            }
            NavisionDBRelation.Relation[] relationArray2 = this.Relaciones.get();
            if ((this.Tablas.Length < 2) && (relationArray2 != null))
            {
                throw new Exception("It's necessary at least 2 tables to join. Assing values to the \"Tables\" property");
            }
            DataTable[] myDataTables = new DataTable[this.Tablas.Length];
            int index = 0;
            for (num = 0; num < this.Tablas.Length; num++)
            {
                if (!this.Relaciones.EsTablaRelacionada(this.Tablas[num].TableName))
                {
                    myDataTables[index] = new DataTable(this.Tablas[num].TableName);
                    this.RellenarTabla(ref myDataTables[index], num);
                    index++;
                }
            }
            if (relationArray2 != null)
            {
                NavisionColumnName = true;
                for (num = 0; num < relationArray2.Length; num++)
                {
                    int num3 = -1;
                    int num4 = -1;
                    for (int i = 0; i < this.Tables.Length; i++)
                    {
                        if (relationArray2[num].Parent_T == this.Tables[i].TableName)
                        {
                            num3 = i;
                        }
                        if (relationArray2[num].Child_T == this.Tables[i].TableName)
                        {
                            num4 = i;
                        }
                    }
                    for (int j = 0; j < relationArray2[num].RelationCol.Length; j++)
                    {
                        int columna = this.Tables[num3].FieldNo(relationArray2[num].RelationCol[j].PColumn);
                        if (!this.ExisteCampoEnVista(num3, columna))
                        {
                            this.AñadirCol(ref tablaColumnas, this.Tables[num3].TableName, relationArray2[num].RelationCol[j].PColumn);
                            this.Tables[num3].AddColumn(columna);
                        }
                        int num8 = this.Tables[num4].FieldNo(relationArray2[num].RelationCol[j].ChColumn);
                        if (!this.ExisteCampoEnVista(num4, num8))
                        {
                            this.AñadirCol(ref tablaColumnas, this.Tables[num4].TableName, relationArray2[num].RelationCol[j].ChColumn);
                            this.Tables[num4].AddColumn(num8);
                        }
                    }
                }
                for (num = 0; num < relationArray2.Length; num++)
                {
                    DataTable table;
                    DataTable table2;
                    DataRelation[] relationArray3;
                    int indexRelPadre = 0;
                    int num10 = 0;
                    switch (relationArray2[num].RelationType)
                    {
                        case JoinType.DataSet_Relation:
                            if (relationArray != null)
                            {
                                break;
                            }
                            relationArray3 = new DataRelation[1];
                            goto Label_0405;

                        case JoinType.Join:
                            table = this.ObtenerPadreRelac(resJoinAux, relationArray2[num].Parent_T, ref indexRelPadre);
                            table2 = this.ObtenerPadreRelac(resJoinAux, relationArray2[num].Child_T, ref num10);
                            resJoinAux = this.Hacer_TipoJoin(resJoinAux, relationArray2[num], table, indexRelPadre, table2, num10);
                            goto Label_0428;

                        case JoinType.Inner_Join:
                            table = this.ObtenerPadreRelac(resJoinAux, relationArray2[num].Parent_T, ref indexRelPadre);
                            table2 = this.ObtenerHijaRelac(resJoinAux, relationArray2[num].Child_T, ref num10);
                            resJoinAux = this.HacerInner_Join(resJoinAux, relationArray2[num], table, indexRelPadre, table2, num10);
                            goto Label_0428;

                        case JoinType.Parent_Outer_Join:
                            table = this.ObtenerPadreRelac(resJoinAux, relationArray2[num].Parent_T, ref indexRelPadre);
                            table2 = this.ObtenerHijaRelac(resJoinAux, relationArray2[num].Child_T, ref num10);
                            resJoinAux = this.HacerParent_Child_OuterJoin(resJoinAux, relationArray2[num], table, indexRelPadre, table2, num10);
                            goto Label_0428;

                        case JoinType.Child_Outer_Join:
                            table = this.ObtenerPadreRelac(resJoinAux, relationArray2[num].Parent_T, ref indexRelPadre);
                            table2 = this.ObtenerHijaRelac(resJoinAux, relationArray2[num].Child_T, ref num10);
                            resJoinAux = this.HacerParent_Child_OuterJoin(resJoinAux, relationArray2[num], table, indexRelPadre, table2, num10);
                            goto Label_0428;

                        default:
                            goto Label_0428;
                    }
                    relationArray3 = new DataRelation[relationArray.Length + 1];
                    for (int k = 0; k < relationArray.Length; k++)
                    {
                        relationArray3[k] = relationArray[k];
                    }
                Label_0405:
                    relationArray3[relationArray3.Length - 1] = this.Hacer_DataSetRelation(ref index, ref myDataTables, relationArray2[num]);
                    relationArray = relationArray3;
                Label_0428:;
                }
                if (resJoinAux != null)
                {
                    for (num = 0; num < resJoinAux.Length; num++)
                    {
                        string str = "";
                        this.BorrarColAñadidaJoin(tablaColumnas, resJoinAux[num].resJoin);
                        for (int m = 0; m < resJoinAux[num].resJoin.Columns.Count; m++)
                        {
                            str = this.EliminarEspGuiPar(resJoinAux[num].resJoin.Columns[m].ColumnName);
                            resJoinAux[num].resJoin.Columns[m].ColumnName = str;
                        }
                        string str2 = this.EliminarGuionYPunto(resJoinAux[num].resJoin.TableName);
                        resJoinAux[num].resJoin.TableName = str2;
                        myDataTables[index++] = resJoinAux[num].resJoin;
                    }
                }
            }
            for (num = 0; num < index; num++)
            {
                if (!NavisionColumnName)
                {
                    for (int n = 0; n < myDataTables[num].Columns.Count; n++)
                    {
                        string cadena = myDataTables[num] + "." + myDataTables[num].Columns[n].ColumnName;
                        cadena = this.EliminarEspGuiPar(cadena);
                        myDataTables[num].Columns[n].ColumnName = cadena;
                    }
                    string str3 = this.EliminarGuionYPunto(myDataTables[num].TableName);
                    myDataTables[num].TableName = str3;
                }
                ds.Tables.Add(myDataTables[num]);
            }
            ds.AcceptChanges();
            if (relationArray != null)
            {
                for (int num14 = 0; num14 < relationArray.Length; num14++)
                {
                    try
                    {
                        ds.Relations.Add(relationArray[num14]);
                    }
                    catch (Exception exception)
                    {
                        string message = exception.Message;
                    }
                }
                ds.AcceptChanges();
            }
        }

        private DataRelation Hacer_DataSetRelation(ref int index, ref DataTable[] myDataTables, NavisionDBRelation.Relation relacion)
        {
            bool flag = false;
            bool flag2 = false;
            if (index > 0)
            {
                for (int j = 0; j < index; j++)
                {
                    if (relacion.Parent_T == myDataTables[j].TableName)
                    {
                        flag = true;
                        break;
                    }
                }
                for (int k = 0; k < index; k++)
                {
                    if (relacion.Parent_T == myDataTables[k].TableName)
                    {
                        flag2 = true;
                        break;
                    }
                }
            }
            if (!flag)
            {
                for (int m = 0; m < this.Tables.Length; m++)
                {
                    if (relacion.Parent_T == this.Tables[m].TableName)
                    {
                        myDataTables[index] = new DataTable(this.Tablas[m].TableName + "DR");
                        this.RellenarTabla(ref myDataTables[index], m);
                        index++;
                    }
                }
            }
            if (!flag2)
            {
                for (int n = 0; n < this.Tables.Length; n++)
                {
                    if (relacion.Child_T == this.Tables[n].TableName)
                    {
                        myDataTables[index] = new DataTable(this.Tablas[n].TableName + "DR");
                        this.RellenarTabla(ref myDataTables[index], n);
                        index++;
                    }
                }
            }
            string[] parentColumnNames = new string[relacion.RelationCol.Length];
            string[] childColumnNames = new string[relacion.RelationCol.Length];
            for (int i = 0; i < relacion.RelationCol.Length; i++)
            {
                parentColumnNames[i] = relacion.RelationCol[i].PColumn;
                childColumnNames[i] = relacion.RelationCol[i].ChColumn;
            }
            return new DataRelation(relacion.Parent_T + "DR." + relacion.Child_T + "DR", relacion.Parent_T + "DR", relacion.Child_T + "DR", parentColumnNames, childColumnNames, false);
        }

        private NavisionDBRelation.ResultJoin[] Hacer_TipoJoin(NavisionDBRelation.ResultJoin[] ResJoinAux, NavisionDBRelation.Relation relacion, DataTable t_padreAux, int indexPadreResultJoin, DataTable t_hijaAux, int indexHijoResultJoin)
        {
            string[] strArray = new string[relacion.RelationCol.Length];
            string[] strArray2 = new string[relacion.RelationCol.Length];
            DataTable table = new DataTable();
            if (t_padreAux == null)
            {
                throw new Exception("Parent table must not be null in Join.");
            }
            if (t_hijaAux == null)
            {
                throw new Exception("Child table must not be null in Join");
            }
            for (int i = 0; i < relacion.RelationCol.Length; i++)
            {
                strArray[i] = relacion.Parent_T + "." + relacion.RelationCol[i].PColumn;
                strArray2[i] = relacion.RelationCol[i].ChColumn;
            }
            for (int j = 0; j < t_hijaAux.Columns.Count; j++)
            {
                t_padreAux.Columns.Add(t_hijaAux.Columns[j].ColumnName, t_hijaAux.Columns[j].DataType);
            }
            table = t_padreAux.Clone();
            table.AcceptChanges();
            t_hijaAux.Columns.Add("Aniadida", Type.GetType("System.Boolean"));
            t_hijaAux.AcceptChanges();
            for (int k = 0; k < t_padreAux.Rows.Count; k++)
            {
                bool flag = false;
                if (t_hijaAux.Rows.Count > 0)
                {
                    for (int n = 0; n < t_hijaAux.Rows.Count; n++)
                    {
                        string columnName;
                        bool flag2 = true;
                        for (int num5 = 0; num5 < relacion.RelationCol.Length; num5++)
                        {
                            string str = relacion.Parent_T + "." + relacion.RelationCol[num5].PColumn;
                            columnName = relacion.Child_T + "." + relacion.RelationCol[num5].ChColumn;
                            if (t_padreAux.Rows[k][str].ToString() != t_hijaAux.Rows[n][columnName].ToString())
                            {
                                flag2 = false;
                            }
                        }
                        if (flag2)
                        {
                            table.ImportRow(t_padreAux.Rows[k]);
                            flag = true;
                            for (int num6 = 0; num6 < t_hijaAux.Columns.Count; num6++)
                            {
                                columnName = t_hijaAux.Columns[num6].ColumnName;
                                if (columnName != "Aniadida")
                                {
                                    table.Rows[table.Rows.Count - 1][columnName] = t_hijaAux.Rows[n][columnName];
                                }
                            }
                            t_hijaAux.Rows[n]["Aniadida"] = true;
                            if (!relacion.distinct)
                            {
                                continue;
                            }
                            break;
                        }
                        if ((n == (t_hijaAux.Rows.Count - 1)) && !flag)
                        {
                            table.ImportRow(t_padreAux.Rows[k]);
                        }
                    }
                }
                else
                {
                    table.ImportRow(t_padreAux.Rows[k]);
                }
            }
            for (int m = 0; m < t_hijaAux.Rows.Count; m++)
            {
                if (t_hijaAux.Rows[m]["Aniadida"] != DBNull.Value)
                {
                    continue;
                }
                for (int num8 = 0; num8 < table.Rows.Count; num8++)
                {
                    bool flag3 = true;
                    for (int num9 = 0; num9 < relacion.RelationCol.Length; num9++)
                    {
                        string str3 = relacion.Parent_T + "." + relacion.RelationCol[num9].PColumn;
                        string str4 = relacion.Child_T + "." + relacion.RelationCol[num9].ChColumn;
                        if (table.Rows[num8][str3].ToString() != t_hijaAux.Rows[m][str4].ToString())
                        {
                            flag3 = false;
                        }
                    }
                    if (flag3)
                    {
                        t_hijaAux.Rows[m]["Aniadida"] = true;
                        break;
                    }
                }
                if (t_hijaAux.Rows[m]["Aniadida"] == DBNull.Value)
                {
                    DataRow row = table.NewRow();
                    string str5 = "";
                    for (int num10 = 0; num10 < t_hijaAux.Columns.Count; num10++)
                    {
                        str5 = t_hijaAux.Columns[num10].ColumnName;
                        if (str5 != "Aniadida")
                        {
                            row[str5] = t_hijaAux.Rows[m][str5];
                        }
                    }
                    table.Rows.Add(row);
                }
            }
            table.AcceptChanges();
            this.ActualizaResultJoin(ref ResJoinAux, ref indexPadreResultJoin, ref indexHijoResultJoin, relacion.Parent_T, relacion.Child_T);
            if (indexPadreResultJoin != -1)
            {
                ResJoinAux[indexPadreResultJoin].resJoin = table;
            }
            if (indexHijoResultJoin != -1)
            {
                ResJoinAux[indexHijoResultJoin].resJoin = table;
            }
            return ResJoinAux;
        }

        private NavisionDBRelation.ResultJoin[] HacerInner_Join(NavisionDBRelation.ResultJoin[] ResJoinAux, NavisionDBRelation.Relation relacion, DataTable t_padreAux, int indexPadreResultJoin, DataTable t_hijaAux, int indexHijoResultJoin)
        {
            string[] strArray = new string[relacion.RelationCol.Length];
            string[] strArray2 = new string[relacion.RelationCol.Length];
            DataTable table = new DataTable();
            if (t_padreAux == null)
            {
                throw new Exception("Parent table must not be null");
            }
            for (int i = 0; i < relacion.RelationCol.Length; i++)
            {
                strArray[i] = relacion.Parent_T + "." + relacion.RelationCol[i].PColumn;
                strArray2[i] = relacion.RelationCol[i].ChColumn;
            }
            if (t_hijaAux != null)
            {
                for (int j = 0; j < t_hijaAux.Columns.Count; j++)
                {
                    t_padreAux.Columns.Add(t_hijaAux.Columns[j].ColumnName, t_hijaAux.Columns[j].DataType);
                }
                table = t_padreAux.Clone();
                table.AcceptChanges();
                for (int k = 0; k < t_padreAux.Rows.Count; k++)
                {
                    if (t_hijaAux.Rows.Count > 0)
                    {
                        for (int m = 0; m < t_hijaAux.Rows.Count; m++)
                        {
                            string columnName;
                            bool flag = true;
                            for (int n = 0; n < relacion.RelationCol.Length; n++)
                            {
                                string str3 = relacion.Parent_T + "." + relacion.RelationCol[n].PColumn;
                                columnName = relacion.Child_T + "." + relacion.RelationCol[n].ChColumn;
                                if (t_padreAux.Rows[k][str3].ToString() != t_hijaAux.Rows[m][columnName].ToString())
                                {
                                    flag = false;
                                }
                            }
                            if (flag)
                            {
                                table.ImportRow(t_padreAux.Rows[k]);
                                for (int num13 = 0; num13 < t_hijaAux.Columns.Count; num13++)
                                {
                                    columnName = t_hijaAux.Columns[num13].ColumnName;
                                    table.Rows[table.Rows.Count - 1][columnName] = t_hijaAux.Rows[m][columnName];
                                }
                                if (relacion.distinct)
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                int index = -1;
                for (int num3 = 0; num3 < this.Tablas.Length; num3++)
                {
                    if (this.Tablas[num3].TableName == relacion.Child_T)
                    {
                        index = num3;
                        break;
                    }
                }
                if (index == -1)
                {
                    throw new Exception("Child table must be add to the adapter.");
                }
                NavisionDBTable table2 = new NavisionDBTable(this.Tablas[index].ConnectionDB, this.Tablas[index].User);
                try
                {
                    for (int num4 = 0; num4 < this.Tablas[index].ColumnsName.Length; num4++)
                    {
                        Type type = this.Tablas[index].ColumnType(this.Tablas[index].Columns[num4]);
                        t_padreAux.Columns.Add(relacion.Child_T + "." + this.Tablas[index].ColumnsName[num4], type);
                    }
                }
                catch (Exception exception)
                {
                    throw exception;
                }
                table = t_padreAux.Clone();
                table.TableName = relacion.Parent_T;
                table.AcceptChanges();
                for (int num5 = 0; num5 < t_padreAux.Rows.Count; num5++)
                {
                    table2.Reset();
                    this.CopiarDBTable(this.Tablas[index], ref table2);
                    for (int num6 = 0; num6 < relacion.RelationCol.Length; num6++)
                    {
                        object obj2 = t_padreAux.Rows[num5][strArray[num6]];
                        if (obj2 is DBNull)
                        {
                            obj2 = " ";
                        }
                        table2.AddFilter(strArray2[num6], (string) obj2);
                    }
                    DataTable myDataTable = new DataTable();
                    this.RellenarTablaGeneral(ref myDataTable, table2, relacion.distinct);
                    if (myDataTable.Rows.Count > 0)
                    {
                        for (int num7 = 0; num7 < myDataTable.Rows.Count; num7++)
                        {
                            table.ImportRow(t_padreAux.Rows[num5]);
                            string str = "";
                            string str2 = "";
                            for (int num8 = 0; num8 < myDataTable.Columns.Count; num8++)
                            {
                                str = myDataTable.Columns[num8].ColumnName;
                                str2 = relacion.Child_T + "." + str;
                                table.Rows[table.Rows.Count - 1][str2] = myDataTable.Rows[num7][str];
                            }
                        }
                    }
                    table.AcceptChanges();
                }
            }
            this.ActualizaResultJoin(ref ResJoinAux, ref indexPadreResultJoin, ref indexHijoResultJoin, relacion.Parent_T, relacion.Child_T);
            if (indexPadreResultJoin != -1)
            {
                ResJoinAux[indexPadreResultJoin].resJoin = table;
            }
            if (indexHijoResultJoin != -1)
            {
                ResJoinAux[indexHijoResultJoin].resJoin = table;
            }
            return ResJoinAux;
        }

        private NavisionDBRelation.ResultJoin[] HacerParent_Child_OuterJoin(NavisionDBRelation.ResultJoin[] ResJoinAux, NavisionDBRelation.Relation relacion, DataTable t_padreAux, int indexPadreResultJoin, DataTable t_hijaAux, int indexHijoResultJoin)
        {
            string[] strArray = new string[relacion.RelationCol.Length];
            string[] strArray2 = new string[relacion.RelationCol.Length];
            DataTable table = new DataTable();
            if (t_padreAux == null)
            {
                throw new Exception("Parent table must not be null");
            }
            for (int i = 0; i < relacion.RelationCol.Length; i++)
            {
                strArray[i] = relacion.Parent_T + "." + relacion.RelationCol[i].PColumn;
                strArray2[i] = relacion.RelationCol[i].ChColumn;
            }
            if (t_hijaAux != null)
            {
                for (int j = 0; j < t_hijaAux.Columns.Count; j++)
                {
                    t_padreAux.Columns.Add(t_hijaAux.Columns[j].ColumnName, t_hijaAux.Columns[j].DataType);
                }
                table = t_padreAux.Clone();
                table.AcceptChanges();
                for (int k = 0; k < t_padreAux.Rows.Count; k++)
                {
                    bool flag = false;
                    if (t_hijaAux.Rows.Count > 0)
                    {
                        for (int m = 0; m < t_hijaAux.Rows.Count; m++)
                        {
                            string columnName;
                            bool flag2 = true;
                            for (int n = 0; n < relacion.RelationCol.Length; n++)
                            {
                                string str4 = relacion.Parent_T + "." + relacion.RelationCol[n].PColumn;
                                columnName = relacion.Child_T + "." + relacion.RelationCol[n].ChColumn;
                                if (t_padreAux.Rows[k][str4].ToString() != t_hijaAux.Rows[m][columnName].ToString())
                                {
                                    flag2 = false;
                                }
                            }
                            if (flag2)
                            {
                                table.ImportRow(t_padreAux.Rows[k]);
                                flag = true;
                                for (int num13 = 0; num13 < t_hijaAux.Columns.Count; num13++)
                                {
                                    columnName = t_hijaAux.Columns[num13].ColumnName;
                                    table.Rows[table.Rows.Count - 1][columnName] = t_hijaAux.Rows[m][columnName];
                                }
                                if (!relacion.distinct)
                                {
                                    continue;
                                }
                                break;
                            }
                            if ((m == (t_hijaAux.Rows.Count - 1)) && !flag)
                            {
                                table.ImportRow(t_padreAux.Rows[k]);
                            }
                        }
                    }
                    else
                    {
                        table.ImportRow(t_padreAux.Rows[k]);
                    }
                }
            }
            else
            {
                int index = -1;
                for (int num3 = 0; num3 < this.Tablas.Length; num3++)
                {
                    if (this.Tablas[num3].TableName == relacion.Child_T)
                    {
                        index = num3;
                        break;
                    }
                }
                if (index == -1)
                {
                    throw new Exception("Child table must be add to the adapter.");
                }
                NavisionDBTable table2 = new NavisionDBTable(this.Tablas[index].ConnectionDB, this.Tablas[index].User);
                try
                {
                    for (int num4 = 0; num4 < this.Tablas[index].ColumnsName.Length; num4++)
                    {
                        Type type = this.Tablas[index].ColumnType(this.Tablas[index].Columns[num4]);
                        t_padreAux.Columns.Add(relacion.Child_T + "." + this.Tablas[index].ColumnsName[num4], type);
                    }
                }
                catch (Exception exception)
                {
                    throw exception;
                }
                table = t_padreAux.Clone();
                table.TableName = relacion.Parent_T;
                table.AcceptChanges();
                for (int num5 = 0; num5 < t_padreAux.Rows.Count; num5++)
                {
                    table2.Reset();
                    this.CopiarDBTable(this.Tablas[index], ref table2);
                    for (int num6 = 0; num6 < relacion.RelationCol.Length; num6++)
                    {
                        object obj2 = t_padreAux.Rows[num5][strArray[num6]];
                        if (obj2 is DBNull)
                        {
                            obj2 = " ";
                        }
                        string valor = obj2.ToString();
                        table2.AddFilter(strArray2[num6], valor);
                    }
                    DataTable myDataTable = new DataTable();
                    this.RellenarTablaGeneral(ref myDataTable, table2, relacion.distinct);
                    if (myDataTable.Rows.Count > 0)
                    {
                        for (int num7 = 0; num7 < myDataTable.Rows.Count; num7++)
                        {
                            table.ImportRow(t_padreAux.Rows[num5]);
                            string str2 = "";
                            string str3 = "";
                            for (int num8 = 0; num8 < myDataTable.Columns.Count; num8++)
                            {
                                str2 = myDataTable.Columns[num8].ColumnName;
                                str3 = relacion.Child_T + "." + str2;
                                table.Rows[table.Rows.Count - 1][str3] = myDataTable.Rows[num7][str2];
                            }
                        }
                    }
                    else
                    {
                        table.ImportRow(t_padreAux.Rows[num5]);
                    }
                    table.AcceptChanges();
                }
            }
            this.ActualizaResultJoin(ref ResJoinAux, ref indexPadreResultJoin, ref indexHijoResultJoin, relacion.Parent_T, relacion.Child_T);
            if (indexPadreResultJoin != -1)
            {
                ResJoinAux[indexPadreResultJoin].resJoin = table;
            }
            if (indexHijoResultJoin != -1)
            {
                ResJoinAux[indexHijoResultJoin].resJoin = table;
            }
            return ResJoinAux;
        }

        private void InicializarTabla(ref DataTable tabla, int Pos)
        {
            try
            {
                if (this.Tables[Pos] == null)
                {
                    throw new Exception("It's necessary to assign value to \"ConnectionDB\" property.");
                }
                if (this.Tables[Pos].TableNo == -1)
                {
                    throw new Exception("It's necessary to assign value to \"Tables\" property.");
                }
                this.Tables[Pos].ConnectionDB.SelectLatestVersion();
                int[] fieldNumbers = this.Tables[Pos].FieldNumbers;
                for (int i = 0; i < fieldNumbers.Length; i++)
                {
                    DataColumn column = new DataColumn(this.Tables[Pos].FieldName(fieldNumbers[i]), Type.GetType("System.String"));
                    tabla.Columns.Add(column);
                }
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message);
            }
        }

        private void InsertarRegistros(DataTable registros)
        {
            try
            {
                if (this.Tables[0] == null)
                {
                    throw new Exception("It's necessary to assign value to \"ConnectionDB\" property.");
                }
                if (this.Tables[0].TableNo == -1)
                {
                    throw new Exception("It's necessary to assign value to \"Tables\" property.");
                }
                this.Tables[0].ConnectionDB.SelectLatestVersion();
                CFrontRecordset recordset = this.Tables[0].ConnectionDB.OpenTable(this.Tables[0].TableNo);
                recordset.LockTable();
                for (int i = 0; i < registros.Rows.Count; i++)
                {
                    recordset.AddNew();
                    for (int j = 0; j < registros.Columns.Count; j++)
                    {
                        string columnName = registros.Columns[j].ColumnName;
                        int num3 = this.Tables[0].FieldNo(columnName);
                        recordset[num3] = registros.Rows[i][j];
                    }
                    recordset.Update();
                }
                recordset.FreeRec();
                recordset.Close();
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message);
            }
        }

        private DataTable ObtenerHijaRelac(NavisionDBRelation.ResultJoin[] ResJoinAux, string nombreTabla, ref int indexRelHijo)
        {
            indexRelHijo = -1;
            new DataTable();
            if (ResJoinAux != null)
            {
                for (int i = 0; i < ResJoinAux.Length; i++)
                {
                    for (int j = 0; j < ResJoinAux[i].tNames.Length; j++)
                    {
                        if (ResJoinAux[i].tNames[j] == nombreTabla)
                        {
                            indexRelHijo = i;
                            return ResJoinAux[i].resJoin;
                        }
                    }
                }
            }
            return null;
        }

        private string[] ObtenerNombresTablaColumna(string NombreColumna, string[] TRelacionadas, DataTable[] TAux)
        {
            int length = NombreColumna.Length;
            int index = 0;
            string[] strArray = new string[2];
            bool flag = false;
            bool flag2 = false;
            bool flag3 = false;
            while (!flag2)
            {
                strArray[1] = "";
                goto Label_0129;
            Label_002D:
                length--;
                if (NombreColumna[length] == '.')
                {
                    flag3 = true;
                }
                else
                {
                    strArray[1] = NombreColumna[length] + strArray[1];
                }
            Label_005A:
                if (!flag3)
                {
                    goto Label_002D;
                }
                flag3 = false;
                int num2 = length - 1;
                strArray[0] = "";
                goto Label_010B;
            Label_0073:
                if (NombreColumna[num2] == '.')
                {
                    flag3 = true;
                }
                else
                {
                    strArray[0] = NombreColumna[num2] + strArray[0];
                }
                num2--;
            Label_00A0:
                if (!flag3 && (num2 > -1))
                {
                    goto Label_0073;
                }
                for (int i = 0; i < TRelacionadas.Length; i++)
                {
                    index = 0;
                    while (index < TAux.Length)
                    {
                        if ((strArray[0] == TAux[index].TableName) && (strArray[0] == TRelacionadas[i]))
                        {
                            flag = true;
                            break;
                        }
                        index++;
                    }
                    if (index < TAux.Length)
                    {
                        break;
                    }
                }
                if (!flag)
                {
                    strArray[0] = '.' + strArray[0];
                }
                flag3 = false;
            Label_010B:
                if (!flag && (num2 > -1))
                {
                    goto Label_00A0;
                }
                if (!flag)
                {
                    strArray[1] = "." + strArray[1];
                }
            Label_0129:
                if (!flag)
                {
                    goto Label_005A;
                }
                int num5 = 0;
                while (num5 < TAux[index].Columns.Count)
                {
                    if (TAux[index].Columns[num5].ColumnName == strArray[1])
                    {
                        break;
                    }
                    num5++;
                }
                if (num5 < TAux[index].Columns.Count)
                {
                    flag2 = true;
                }
                else
                {
                    flag3 = false;
                }
            }
            return strArray;
        }

        private DataTable ObtenerPadreRelac(NavisionDBRelation.ResultJoin[] ResJoinAux, string nombreTabla, ref int indexRelPadre)
        {
            DataTable myDataTable = new DataTable(nombreTabla);
            indexRelPadre = -1;
            if (ResJoinAux == null)
            {
                for (int k = 0; k < this.Tablas.Length; k++)
                {
                    if (nombreTabla == this.Tablas[k].TableName)
                    {
                        this.RellenarTabla(ref myDataTable, k);
                        for (int m = 0; m < myDataTable.Columns.Count; m++)
                        {
                            myDataTable.Columns[m].ColumnName = myDataTable.TableName + "." + myDataTable.Columns[m].ColumnName;
                        }
                        myDataTable.AcceptChanges();
                        return myDataTable;
                    }
                }
                return myDataTable;
            }
            for (int i = 0; i < ResJoinAux.Length; i++)
            {
                for (int n = 0; n < ResJoinAux[i].tNames.Length; n++)
                {
                    if (ResJoinAux[i].tNames[n] == nombreTabla)
                    {
                        indexRelPadre = i;
                        return ResJoinAux[i].resJoin;
                    }
                }
            }
            for (int j = 0; j < this.Tablas.Length; j++)
            {
                if (nombreTabla == this.Tablas[j].TableName)
                {
                    this.RellenarTabla(ref myDataTable, j);
                    for (int num6 = 0; num6 < myDataTable.Columns.Count; num6++)
                    {
                        myDataTable.Columns[num6].ColumnName = myDataTable.TableName + "." + myDataTable.Columns[num6].ColumnName;
                    }
                    myDataTable.AcceptChanges();
                    return myDataTable;
                }
            }
            return myDataTable;
        }

        private CFrontRecordset OpenTable(int num)
        {
            CFrontRecordset recordset;
            try
            {
                this.Tablas[num].ConnectionDB.AllowAll();
                if (this.Tablas[num].TableNo != -1)
                {
                    return this.Tablas[num].ConnectionDB.OpenTable(this.Tablas[num].TableNo);
                }
                recordset = this.Tablas[num].ConnectionDB.OpenTable(this.Tablas[num].TableName);
            }
            catch (Exception exception)
            {
                throw new Exception("Error: " + exception.Message);
            }
            return recordset;
        }

        private void RealizarUpdate(NavisionDBTable TAux)
        {
            int num;
            CFrontRecordset recordset;
            object[,] updates = TAux.Updates;
            if (updates != null)
            {
                TAux.ConnectionDB.SelectLatestVersion();
                recordset = TAux.ConnectionDB.OpenTable(TAux.TableNo);
                if (TAux.Key != null)
                {
                    recordset.SetCurrentKey(TAux.Key);
                }
                object[,] filters = TAux.Filters;
                if (filters != null)
                {
                    for (num = 0; num < (filters.Length / 2); num++)
                    {
                        recordset.SetFilter(Convert.ToInt32(filters[num, 0]), Convert.ToString(filters[num, 1]));
                    }
                }
                recordset.MoveFirst();
                if (!recordset.EOF)
                {
                    try
                    {
                        recordset.LockTable();
                        do
                        {
                            recordset.Edit();
                            for (num = 0; num < (updates.Length / 2); num++)
                            {
                                recordset[Convert.ToInt32(updates[num, 0])] = updates[num, 1];
                            }
                            recordset.Update();
                        }
                        while (recordset.MoveNext());
                    }
                    catch
                    {
                        throw new Exception("ERROR: It have not been possible to do the update in the table " + TAux.TableName);
                    }
                }
                recordset.FreeRec();
                recordset.Close();
            }
            updates = TAux.Inserts;
            if (updates != null)
            {
                TAux.ConnectionDB.SelectLatestVersion();
                recordset = TAux.ConnectionDB.OpenTable(TAux.TableNo);
                try
                {
                    recordset.LockTable();
                    recordset.AddNew();
                    for (num = 0; num < (updates.Length / 2); num++)
                    {
                        recordset[Convert.ToInt32(updates[num, 0])] = updates[num, 1];
                    }
                    recordset.Update();
                }
                catch
                {
                    throw new Exception("ERROR: It have not been possible to do the insert in the table " + TAux.TableName);
                }
                recordset.FreeRec();
                recordset.Close();
            }
        }

        private void RellenarTabla(ref DataTable myDataTable, int num)
        {
            DataRow row;
            int[] fieldNumbers;
            string[] columnsName = null;
            int num2;
            int fieldCount;
            if (((this.Tablas[num].Columns == null) && (this.Tablas[num].ColumnsName == null)) && !this.Tablas[num].NoColumn)
            {
                fieldNumbers = this.Tablas[num].FieldNumbers;
                fieldCount = this.Tablas[num].FieldCount;
            }
            else
            {
                fieldCount = this.Tablas[num].Columns.Length;
                fieldNumbers = this.Tablas[num].Columns;
                columnsName = this.Tablas[num].ColumnsName;
            }
            this.Tablas[num].ConnectionDB.SelectLatestVersion();
            CFrontRecordset recordset = this.OpenTable(num);
            NavisionFieldType[] typeArray = new NavisionFieldType[fieldCount];
            for (num2 = 0; num2 < fieldCount; num2++)
            {
                string str;
                string typeName = "";
                if (columnsName == null)
                {
                    str = this.Tablas[num].FieldName(fieldNumbers[num2]);
                }
                else
                {
                    str = columnsName[num2];
                }
                switch (recordset.FieldType(fieldNumbers[num2], ref typeArray[num2]))
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
                        throw new Exception("Error: Unknown data type -> " + recordset.FieldType(fieldNumbers[num2], ref typeArray[num2]).ToString());
                }
                DataColumn column = new DataColumn(str, Type.GetType(typeName));
                myDataTable.Columns.Add(column);
            }
            if (this.Tablas[num].Key != null)
            {
                recordset.SetCurrentKey(this.Tablas[num].Key);
            }
            object[,] filters = this.Tablas[num].Filters;
            if (filters != null)
            {
                for (num2 = 0; num2 < (filters.Length / 2); num2++)
                {
                    recordset.SetFilter(Convert.ToInt32(filters[num2, 0]), Convert.ToString(filters[num2, 1]));
                }
            }
            if (this.Tablas[num].Reverse)
            {
                recordset.MoveLast();
            }
            else
            {
                recordset.MoveFirst();
            }
            if (recordset.EOF)
            {
                goto Label_0443;
            }
        Label_03D8:
            row = myDataTable.NewRow();
            for (num2 = 0; num2 < fieldCount; num2++)
            {
                row[num2] = recordset[fieldNumbers[num2]];
            }
            myDataTable.Rows.Add(row);
            if (!this.Tablas[num].firstRow)
            {
                if (this.Tablas[num].Reverse)
                {
                    if (recordset.MovePrevious())
                    {
                        goto Label_03D8;
                    }
                }
                else if (recordset.MoveNext())
                {
                    goto Label_03D8;
                }
            }
        Label_0443:
            myDataTable.AcceptChanges();
            recordset.FreeRec();
            recordset.Close();
        }

        private void RellenarTablaGeneral(ref DataTable myDataTable, NavisionDBTable tabla, bool distinct)
        {
            DataRow row;
            int[] fieldNumbers;
            string[] columnsName = null;
            int num;
            int fieldCount;
            if (((tabla.Columns == null) && (tabla.ColumnsName == null)) && !tabla.NoColumn)
            {
                fieldNumbers = tabla.FieldNumbers;
                fieldCount = tabla.FieldCount;
            }
            else
            {
                fieldCount = tabla.Columns.Length;
                fieldNumbers = tabla.Columns;
                columnsName = tabla.ColumnsName;
            }
            tabla.ConnectionDB.SelectLatestVersion();
            CFrontRecordset recordset = tabla.ConnectionDB.OpenTable(tabla.TableNo);
            NavisionFieldType[] typeArray = new NavisionFieldType[fieldCount];
            for (num = 0; num < fieldCount; num++)
            {
                string str;
                string typeName = "";
                if (columnsName == null)
                {
                    str = tabla.FieldName(fieldNumbers[num]);
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
                myDataTable.Columns.Add(column);
            }
            if (tabla.Key != null)
            {
                recordset.SetCurrentKey(tabla.Key);
            }
            object[,] filters = tabla.Filters;
            if (filters != null)
            {
                for (num = 0; num < (filters.Length / 2); num++)
                {
                    recordset.SetFilter(Convert.ToInt32(filters[num, 0]), Convert.ToString(filters[num, 1]));
                }
            }
            if (tabla.Reverse)
            {
                recordset.MoveLast();
            }
            else
            {
                recordset.MoveFirst();
            }
            if (recordset.EOF)
            {
                goto Label_03E0;
            }
        Label_0380:
            row = myDataTable.NewRow();
            for (num = 0; num < fieldCount; num++)
            {
                row[num] = recordset[fieldNumbers[num]];
            }
            myDataTable.Rows.Add(row);
            if (!distinct && !tabla.firstRow)
            {
                if (tabla.Reverse)
                {
                    if (recordset.MovePrevious())
                    {
                        goto Label_0380;
                    }
                }
                else if (recordset.MoveNext())
                {
                    goto Label_0380;
                }
            }
        Label_03E0:
            myDataTable.AcceptChanges();
            recordset.FreeRec();
            recordset.Close();
        }

        private void RellenarTablaToString(ref DataTable myDataTable, int num)
        {
            DataColumn column;
            int[] fieldNumbers;
            string[] columnsName = null;
            int num2;
            int fieldCount;
            if (((this.Tablas[num].Columns == null) && (this.Tablas[num].ColumnsName == null)) && !this.Tablas[num].NoColumn)
            {
                fieldNumbers = this.Tablas[num].FieldNumbers;
                fieldCount = this.Tablas[num].FieldCount;
            }
            else
            {
                fieldCount = this.Tablas[num].Columns.Length;
                fieldNumbers = this.Tablas[num].Columns;
                columnsName = this.Tablas[num].ColumnsName;
            }
            if (columnsName == null)
            {
                for (num2 = 0; num2 < fieldCount; num2++)
                {
                    column = new DataColumn(this.Tablas[num].FieldName(fieldNumbers[num2]), Type.GetType("System.String"));
                    myDataTable.Columns.Add(column);
                }
            }
            else
            {
                num2 = 0;
                while (num2 < fieldCount)
                {
                    column = new DataColumn(columnsName[num2], Type.GetType("System.String"));
                    myDataTable.Columns.Add(column);
                    num2++;
                }
            }
            this.Tablas[num].ConnectionDB.SelectLatestVersion();
            CFrontRecordset recordset = this.OpenTable(num);
            if (this.Tablas[num].Key != null)
            {
                recordset.SetCurrentKey(this.Tablas[num].Key);
            }
            object[,] filters = this.Tablas[num].Filters;
            if (filters != null)
            {
                num2 = 0;
                while (num2 < (filters.Length / 2))
                {
                    recordset.SetFilter(Convert.ToInt32(filters[num2, 0]), Convert.ToString(filters[num2, 1]));
                    num2++;
                }
            }
            recordset.MoveFirst();
            if (!recordset.EOF)
            {
                do
                {
                    DataRow row = myDataTable.NewRow();
                    for (num2 = 0; num2 < fieldCount; num2++)
                    {
                        row[Convert.ToInt32(num2)] = recordset[fieldNumbers[num2]];
                    }
                    myDataTable.Rows.Add(row);
                }
                while (!this.Tablas[num].firstRow && (recordset.MoveStep(1) != 0));
            }
            myDataTable.AcceptChanges();
            recordset.FreeRec();
            recordset.Close();
        }

        public void Update()
        {
            if (!this.ValidarUsuario())
            {
                throw new Exception("Nonvalid user.");
            }
            if (this.Actualizaciones.Columns.Count != 0)
            {
                this.ActualizarRegistros(this.Actualizaciones);
            }
            if (this.Inserciones.Columns.Count != 0)
            {
                this.InsertarRegistros(this.Inserciones);
            }
            if (this.Eliminaciones.Columns.Count != 0)
            {
                this.EliminarRegistros(this.Eliminaciones);
            }
            for (int i = 0; i < this.Tablas.Length; i++)
            {
                this.RealizarUpdate(this.Tablas[i]);
            }
        }

        private bool ValidarUsuario()
        {
            if (this.Tablas.Length == 0)
            {
                return false;
            }
            for (int i = 0; i < this.Tablas.Length; i++)
            {
                if ((this.Tablas[i].TableNo == -1) && (this.Tablas[i].TableName == ""))
                {
                    return false;
                }
            }
            return true;
        }

        public DataSet DeleteItem
        {
            set
            {
                if (!this.ValidarUsuario())
                {
                    throw new Exception("Nonvalid user.");
                }
                if (value != null)
                {
                    if (this.Eliminaciones.Columns.Count == 0)
                    {
                        this.InicializarTabla(ref this.Eliminaciones, 0);
                    }
                    this.AñadirFila(ref this.Eliminaciones, value);
                }
            }
        }

        public DataSet InsertItem
        {
            set
            {
                if (!this.ValidarUsuario())
                {
                    throw new Exception("Nonvalid user.");
                }
                if (value != null)
                {
                    if (this.Inserciones.Columns.Count == 0)
                    {
                        this.InicializarTabla(ref this.Inserciones, 0);
                    }
                    this.AñadirFila(ref this.Inserciones, value);
                }
            }
        }

        public object[,] Relations
        {
            get
            {
                return this.Relaciones.getRelationString();
            }
        }

        public NavisionDBTable[] Tables
        {
            get
            {
                return this.Tablas;
            }
        }

        public DataSet UpdateItem
        {
            set
            {
                if (!this.ValidarUsuario())
                {
                    throw new Exception("Nonvalid user.");
                }
                if (value != null)
                {
                    if (this.Actualizaciones.Columns.Count == 0)
                    {
                        this.InicializarTabla(ref this.Actualizaciones, 0);
                    }
                    this.AñadirFila(ref this.Actualizaciones, value);
                }
            }
        }

        public enum JoinType
        {
            DataSet_Relation,
            Join,
            Inner_Join,
            Parent_Outer_Join,
            Child_Outer_Join
        }
    }
}

