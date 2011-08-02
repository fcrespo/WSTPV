namespace NavisionDB
{
    using System;

    internal class TableKey
    {
        public readonly int[] fields;
        public readonly int[] sumFields;

        public TableKey(int[] fields, int[] sumFields)
        {
            this.fields = fields;
            this.sumFields = sumFields;
        }
    }
}

