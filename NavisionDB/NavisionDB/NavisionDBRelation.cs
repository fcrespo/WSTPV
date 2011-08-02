namespace NavisionDB
{
    using System;
    using System.Data;
    using System.Runtime.InteropServices;

    internal class NavisionDBRelation
    {
        private Relation[] Relaciones;

        public void add(Relation myRelation)
        {
            int index = -1;
            bool flag = true;
            if (this.Relaciones == null)
            {
                this.Relaciones = new Relation[1];
                index = 0;
            }
            else
            {
                for (int i = 0; i < this.Relaciones.Length; i++)
                {
                    if ((myRelation.Parent_T == this.Relaciones[i].Parent_T) && (myRelation.Child_T == this.Relaciones[i].Child_T))
                    {
                        if (myRelation.RelationType != this.Relaciones[i].RelationType)
                        {
                            throw new Exception("Already exits a relation with table " + this.Relaciones[i].Parent_T + "and table " + this.Relaciones[i].Child_T + " with a different relation type.");
                        }
                        index = i;
                        flag = false;
                        break;
                    }
                }
                for (int j = 0; j < this.Relaciones.Length; j++)
                {
                    if ((myRelation.Parent_T == this.Relaciones[j].Child_T) && (myRelation.Child_T == this.Relaciones[j].Parent_T))
                    {
                        throw new Exception("There is a relation with Parent table " + this.Relaciones[j].Parent_T + " and Child Table " + this.Relaciones[j].Child_T + ". The relations with Parent table " + this.Relaciones[j].Child_T + " and Child Table " + this.Relaciones[j].Parent_T + "are not allowed.");
                    }
                }
                if (index == -1)
                {
                    Relation[] relationArray = new Relation[this.Relaciones.Length + 1];
                    for (int k = 0; k < (relationArray.Length - 1); k++)
                    {
                        relationArray[k] = this.Relaciones[k];
                        relationArray[k].RelationCol = new RelationColumns_t[this.Relaciones[k].RelationCol.Length];
                        for (int m = 0; m < this.Relaciones[k].RelationCol.Length; m++)
                        {
                            relationArray[k].RelationCol[m] = this.Relaciones[k].RelationCol[m];
                        }
                    }
                    this.Relaciones = relationArray;
                    index = this.Relaciones.Length - 1;
                }
            }
            if (flag)
            {
                this.Relaciones[index].RelationType = myRelation.RelationType;
                this.Relaciones[index].distinct = myRelation.distinct;
                this.Relaciones[index].RelationCol = new RelationColumns_t[1];
                if (myRelation.RelationType == NavisionDBAdapter.JoinType.Child_Outer_Join)
                {
                    this.Relaciones[index].Parent_T = myRelation.Child_T;
                    this.Relaciones[index].Child_T = myRelation.Parent_T;
                    this.Relaciones[index].RelationCol[0].ChColumn = myRelation.RelationCol[0].PColumn;
                    this.Relaciones[index].RelationCol[0].PColumn = myRelation.RelationCol[0].ChColumn;
                }
                else
                {
                    this.Relaciones[index].Parent_T = myRelation.Parent_T;
                    this.Relaciones[index].Child_T = myRelation.Child_T;
                    this.Relaciones[index].RelationCol[0] = myRelation.RelationCol[0];
                }
            }
            else
            {
                RelationColumns_t[] _tArray = new RelationColumns_t[this.Relaciones[index].RelationCol.Length + 1];
                for (int n = 0; n < (_tArray.Length - 1); n++)
                {
                    _tArray[n] = this.Relaciones[index].RelationCol[n];
                }
                this.Relaciones[index].RelationCol = _tArray;
                if (myRelation.RelationType == NavisionDBAdapter.JoinType.Child_Outer_Join)
                {
                    this.Relaciones[index].RelationCol[this.Relaciones[index].RelationCol.Length - 1].ChColumn = myRelation.RelationCol[0].PColumn;
                    this.Relaciones[index].RelationCol[this.Relaciones[index].RelationCol.Length - 1].PColumn = myRelation.RelationCol[0].ChColumn;
                }
                else
                {
                    this.Relaciones[index].RelationCol[this.Relaciones[index].RelationCol.Length - 1].ChColumn = myRelation.RelationCol[0].ChColumn;
                    this.Relaciones[index].RelationCol[this.Relaciones[index].RelationCol.Length - 1].PColumn = myRelation.RelationCol[0].PColumn;
                }
            }
        }

        public bool EsTablaRelacionada(string tablaName)
        {
            if ((this.Relaciones != null) && (this.Relaciones.Length != 0))
            {
                for (int i = 0; i < this.Relaciones.Length; i++)
                {
                    if ((this.Relaciones[i].Parent_T == tablaName) || (this.Relaciones[i].Child_T == tablaName))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public Relation[] get()
        {
            return this.Relaciones;
        }

        public string[,] getRelationString()
        {
            if ((this.Relaciones == null) || (this.Relaciones.Length == 0))
            {
                return null;
            }
            string[,] strArray = null;
            int num = 0;
            for (int i = 0; i < this.Relaciones.Length; i++)
            {
                num += this.Relaciones[i].RelationCol.Length;
            }
            if (num == 0)
            {
                return strArray;
            }
            string[,] strArray2 = new string[num, 6];
            num = 0;
            for (int j = 0; j < this.Relaciones.Length; j++)
            {
                for (int k = 0; k < this.Relaciones[j].RelationCol.Length; k++)
                {
                    strArray2[num, 0] = this.Relaciones[j].Parent_T;
                    strArray2[num, 1] = this.Relaciones[j].RelationCol[k].PColumn;
                    strArray2[num, 2] = this.Relaciones[j].Child_T;
                    strArray2[num, 3] = this.Relaciones[j].RelationCol[k].ChColumn;
                    strArray2[num, 4] = this.Relaciones[j].RelationType.ToString();
                    strArray2[num, 5] = this.Relaciones[j].distinct.ToString();
                    num++;
                }
            }
            return strArray2;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct Relation
        {
            public string Parent_T;
            public string Child_T;
            public NavisionDBAdapter.JoinType RelationType;
            public NavisionDBRelation.RelationColumns_t[] RelationCol;
            public bool distinct;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct RelationColumns_t
        {
            public string PColumn;
            public string ChColumn;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct ResultJoin
        {
            public string[] tNames;
            public DataTable resJoin;
            public void AniadeTablaJoin(string nombreTabla)
            {
                string[] strArray = new string[this.tNames.Length + 1];
                for (int i = 0; i < this.tNames.Length; i++)
                {
                    strArray[i] = this.tNames[i];
                }
                strArray[this.tNames.Length] = nombreTabla;
                this.tNames = strArray;
            }
        }
    }
}

