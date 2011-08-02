namespace NavisionDB
{
    using System;

    public class GeneradorIDMsg
    {
        private static long numId;

        public static string NewMsgId()
        {
            numId += 1L;
            return ("NasId_" + numId.ToString("X8"));
        }
    }
}

