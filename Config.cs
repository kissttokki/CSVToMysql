using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ServerTableSync
{
    internal class Config
    {
        public string CLIENT_DATATABLE_PATH { get; set; }
        public string LOCAL_DATATABLE_PATH { get; set; }

        public string MYSQL_USER { get; set; } = "root";
        public string MYSQL_PASSWORD { get; set; } = "password";
        public string MYSQL_IP { get; set; } = "127.0.0.1";
        public uint MYSQL_PORT { get; set; } = 3306;
        public string MYSQL_SCHEMA { get; set; } = "schemaname";

        public List<TableNameMapper> TABLE_MAP { get; set; }

        public class TableNameMapper
        {
            public string csvName;
            public string dbName;
        }


        public static Config Default
        {
            get
            {
                if (_Default == null)
                {
                    var info = new FileInfo($"{new FileInfo(System.Environment.ProcessPath).Directory.FullName}/config.json");

                    if (info.Exists == true)
                    {
                        _Default = JsonConvert.DeserializeObject<Config>(File.ReadAllText($"{new FileInfo(System.Environment.ProcessPath).Directory.FullName}/config.json"));
                    }
                    else
                    {
                        _Default = new Config();
                        _Default.TABLE_MAP = new List<TableNameMapper>();
                        File.WriteAllText(info.FullName, JsonConvert.SerializeObject(_Default, Formatting.Indented));
                    }

                }

                return _Default;
            }
        }
        private static Config _Default;


        public void Save()
        {
            var info = new FileInfo($"{new FileInfo(System.Environment.ProcessPath).Directory.FullName}/config.json");
            File.WriteAllText(info.FullName, JsonConvert.SerializeObject(_Default, Formatting.Indented));
        }
    }
}
