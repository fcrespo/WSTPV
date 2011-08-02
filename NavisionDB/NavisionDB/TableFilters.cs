namespace NavisionDB
{
    using System;

    internal class TableFilters
    {
        private object[,] Filtros;

        public void add(object[,] filtro)
        {
            if (this.Filtros == null)
            {
                this.Filtros = new object[,] { { filtro[0, 0], filtro[0, 1] } };
            }
            else
            {
                object[,] filtros = new object[this.Filtros.Length / 2, 2];
                filtros = this.Filtros;
                this.Filtros = new object[(this.Filtros.Length / 2) + 1, 2];
                for (int i = 0; i < (filtros.Length / 2); i++)
                {
                    this.Filtros[i, 0] = filtros[i, 0];
                    this.Filtros[i, 1] = filtros[i, 1];
                }
                this.Filtros[filtros.Length / 2, 0] = filtro[0, 0];
                this.Filtros[filtros.Length / 2, 1] = filtro[0, 1];
            }
        }

        public object[,] get()
        {
            return this.Filtros;
        }
    }
}

