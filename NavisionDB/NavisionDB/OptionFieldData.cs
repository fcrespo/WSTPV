namespace NavisionDB
{
    using System;

    internal class OptionFieldData
    {
        protected readonly int varIntValue;
        protected readonly string varStringValue;

        public OptionFieldData(int intValue, string stringValue)
        {
            this.varIntValue = intValue;
            this.varStringValue = stringValue;
        }

        public override string ToString()
        {
            return this.StringValue;
        }

        public int IntValue
        {
            get
            {
                return this.varIntValue;
            }
        }

        public string StringValue
        {
            get
            {
                return this.varStringValue;
            }
        }
    }
}

