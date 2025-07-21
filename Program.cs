using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;

namespace ServerTableSync
{
    internal class Program
    {
        private static Regex _DataRegex = new Regex(",(?=(?:(?:[^\"]*\"[^\"]*\")*[^\"]*$))", RegexOptions.Compiled);
        private static Regex _NewlineRegex = new Regex(@"(?:(?:[^""\r\n]+|""[^""]*"")+)(?=\r\n|\n|$)", RegexOptions.Compiled);

        /// <summary>
        /// CS파일 나누기 귀찮아서 한번에 그냥 다 처리함.
        /// 나중에 정리 필요.
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            string address = Config.Default.MYSQL_IP;
            uint port = Config.Default.MYSQL_PORT;
            string userName = Config.Default.MYSQL_USER;
            string password = Config.Default.MYSQL_PASSWORD;


            if (args.Length > 0)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    string arg = args[i];

                    if (arg == "-port")
                    {
                        port = uint.Parse(args[i + 1]);
                        i++;
                    }
                    else if (arg == "-address")
                    {
                        address = args[i + 1];
                        i++;
                    }
                    else if (arg == "-username")
                    {
                        userName = args[i + 1];
                        i++;
                    }
                    else if (arg == "-password")
                    {
                        password = args[i + 1];
                        i++;
                    }
                }
            }


            #region 폴더 초기화 / XLSX 파일들 설정
            DirectoryInfo tableDir = null;
            List<FileInfo> files = new List<FileInfo>();

            if (string.IsNullOrEmpty(Config.Default.CLIENT_DATATABLE_PATH) == false)
            {
                DirectoryInfo exportFolder = new DirectoryInfo(Config.Default.CLIENT_DATATABLE_PATH);

                if (exportFolder.Exists == false)
                {
                    Console.WriteLine($"{exportFolder.FullName} 경로에 폴더가 없어 현재 폴더로 타겟을 지정합니다.");

                    exportFolder = new FileInfo(System.Environment.ProcessPath).Directory;
                }
                tableDir = exportFolder;
            }
            else
            {
                Console.WriteLine($"설정된 경로가 없어 현재 폴더로 진행합니다.");
                tableDir = new FileInfo(System.Environment.ProcessPath).Directory;
            }

            if (tableDir.Exists == false)
                tableDir.Create();

            files.AddRange(tableDir.GetFiles());
            #endregion


            if (string.IsNullOrEmpty(Config.Default.LOCAL_DATATABLE_PATH) == false)
            {
                DirectoryInfo exportFolder = new DirectoryInfo(Config.Default.LOCAL_DATATABLE_PATH);

                if (exportFolder.Exists == false)
                {
                    Console.WriteLine($"{exportFolder.FullName} 경로에 폴더가 없어 현재 폴더로 타겟을 지정합니다.");

                    exportFolder = new FileInfo(System.Environment.ProcessPath).Directory;
                }
                tableDir = exportFolder;
            }
            else
            {
                Console.WriteLine($"설정된 경로가 없어 현재 폴더로 진행합니다.");
                tableDir = new FileInfo(System.Environment.ProcessPath).Directory;
            }

            if (tableDir.Exists == false)
                tableDir.Create();


            ///서버용 테이블 읽어옴
            files.AddRange(tableDir.GetFiles());



            List<(FileInfo, string)> targets = new ();

            foreach (var file in files.Where(t => t.Extension.ToLower() == ".csv"))
            {
                var data = Config.Default.TABLE_MAP.FirstOrDefault(t => t.csvName == file.Name.Remove(file.Name.Length - 4, 4));

                if (data == null)
                    continue;

                targets.Add((file, data.dbName));
            }



            Console.WriteLine("================= 읽힌 테이블 리스트 =================");
            foreach (var t in targets)
            {
                Console.WriteLine(t);
            }
            Console.WriteLine("================= ================ =================");



            Config.Default.Save();

            var connectionAddress = string.Format("Server={0};Port={1};Database={2};Uid={3};Pwd={4}", address, port, Config.Default.MYSQL_SCHEMA, userName, password);

            using (MySqlConnection mysql = new MySqlConnection(connectionAddress))
            {
                mysql.Open();

                ///meta_로시작하는 db들 가져오기

                var res = Execute($"SHOW TABLES from `{Config.Default.MYSQL_SCHEMA}`;", mysql);

                List<string> dataTablesOnMysql = res[0];
                foreach (var db in dataTablesOnMysql)
                {
                    if (db.StartsWith("meta_") == true)
                    {
                        if (Config.Default.TABLE_MAP.Any(t => t.dbName == db) == false)
                        {
                            Config.Default.TABLE_MAP.Add(new Config.TableNameMapper() { dbName = db });
                        }
                        Console.WriteLine(db);
                    }
                }

                Config.Default.TABLE_MAP = Config.Default.TABLE_MAP.OrderBy(t => t.dbName).ToList();
                Config.Default.Save();



                foreach ((FileInfo, string) item in targets)
                {
                    if (dataTablesOnMysql.Contains(item.Item2) == false)
                    {
                        Console.WriteLine($"[WARNING] 해당 테이블이 Mysql상에 없음.. {item.Item2}");
                        continue;
                    }

                    ExecuteNonQuery($"TRUNCATE `{Config.Default.MYSQL_SCHEMA}`.`{item.Item2}`", mysql);

                    HashSet<string> dbCols = Execute($"SHOW COLUMNS FROM `{Config.Default.MYSQL_SCHEMA}`.`{item.Item2}`", mysql)[0].ToHashSet();

                    var csvData = File.ReadAllText(item.Item1.FullName);

                    string[] lines = _NewlineRegex.Matches(csvData)
                                    //.Where(m => m.Success && !string.IsNullOrWhiteSpace(m.Value))
                                    .Select(m => m.Value)
                                    .ToArray();

                    // Header 처리 후 columns 딕셔너리 및 Reflection 정보 캐싱
                    Dictionary<int, string> columns = [];
                    const int DATA_START_ROW = 2; // 실제 데이터 시작 인덱스

                    string[] headerCells = lines[0].Split(',');
                    for (int col = 0; col < headerCells.Length; col++)
                    {
                        string header = headerCells[col].Trim();

                        if (dbCols.Contains(header) == true)
                            columns[col] = header;
                    }

                    var lineCount = lines.Length;

                    string[] typeCells = lines[1].Split(',');

                    Console.WriteLine($"INSERT INTO `{Config.Default.MYSQL_SCHEMA}`.`{item.Item2}` ({string.Join(',', columns.Values)}) VALUES ... cnt : {lineCount - DATA_START_ROW}");

                    ///1만개당 컷 (PacketSize 이슈)
                    var maxCnt = (lineCount - DATA_START_ROW) / 10000;

                    var dataList = lines.Skip(2).ToList();

                    for (int splitCnt = 0; splitCnt <= maxCnt; splitCnt++)
                    {
                        StringBuilder sCommand = new StringBuilder($"INSERT INTO `{Config.Default.MYSQL_SCHEMA}`.`{item.Item2}` ({string.Join(',', columns.Values)}) VALUES ");
                        List<string> Rows = new List<string>();


                        var dataLines = dataList.Skip(splitCnt * 10000).Take(10000);

                        foreach(string? line in dataLines)
                        {
                            if (string.IsNullOrEmpty(line) == true || line.Trim().All(c => c == ','))
                                continue;

                            // Regex를 사용하여 셀 분리
                            string[] cells = _DataRegex.Split(line);

                            // 빈 행인지 확인
                            if (cells.All(cell => string.IsNullOrWhiteSpace(cell)))
                                continue;


                            var datas = new List<string>();

                            for (int i1 = 0; i1 < cells.Length; i1++)
                            {
                                if (columns.ContainsKey(i1) == false) continue;

                                string cell = cells[i1].Trim('\"');

                                if (string.IsNullOrEmpty(cell) == true)
                                {
                                    switch (typeCells[i1])
                                    {
                                        case "int":
                                            cell = default(int).ToString();
                                            datas.Add(string.Format("{0}", MySqlHelper.EscapeString(cell)));
                                            break;
                                        case "float":
                                            cell = default(float).ToString();
                                            datas.Add(string.Format("{0}", MySqlHelper.EscapeString(cell)));
                                            break;
                                        case "long":
                                            cell = default(long).ToString();
                                            datas.Add(string.Format("{0}", MySqlHelper.EscapeString(cell)));
                                            break;
                                        case "double":
                                            cell = default(double).ToString();
                                            datas.Add(string.Format("{0}", MySqlHelper.EscapeString(cell)));
                                            break;
                                        case "uint":
                                            cell = default(uint).ToString();
                                            datas.Add(string.Format("{0}", MySqlHelper.EscapeString(cell)));
                                            break;
                                        default:
                                            cell = "DEFAULT";
                                            datas.Add(string.Format("{0}", MySqlHelper.EscapeString(cell)));
                                            break;
                                    }
                                }
                                else if (cell.ToLower() == "true")
                                {
                                    cell = "1";
                                    datas.Add(string.Format("{0}", MySqlHelper.EscapeString(cell)));
                                }
                                else if (cell.ToLower() == "false")
                                {
                                    cell = "0";
                                    datas.Add(string.Format("{0}", MySqlHelper.EscapeString(cell)));
                                }
                                else
                                    datas.Add(string.Format("'{0}'", MySqlHelper.EscapeString(cell)));
                            }

                            Rows.Add($"( {string.Join(',', datas)})");
                        }



                        sCommand.Append(string.Join(",", Rows));
                        ExecuteNonQuery(sCommand.ToString(), mysql);
                    }
                }
            }

        }


        static void ExecuteNonQuery(string command, MySqlConnection con)
        {
            //Console.WriteLine(command);
            using (MySqlCommand com = new MySqlCommand(command, con))
            {
                com.ExecuteNonQuery();
            }
        }
        static Dictionary<int, List<string>> Execute(string command, MySqlConnection con)
        {
            Console.WriteLine(command);
            var result = new Dictionary<int, List<string>>();
            using (MySqlCommand com = new MySqlCommand(command, con))
            {
                using (MySqlDataReader reader = com.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (result.ContainsKey(i) == false)
                            {
                                result[i] = new List<string>();
                            }

                            result[i].Add(reader[i].ToString());
                        }
                    }
                }
            }

            return result;
        }

    }
}
