using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Specialized;
using System.IO;
using System.Text.RegularExpressions;
using System.Net;
using SevenZipExtractor;
using System.Data;
using System.Data.SqlClient;

namespace GeoSirene
{
    class Program
    {
        //string strConn = "Server=" + ConfigurationManager.AppSettings.Get("DBServer") +
        //                 ";Database=" + ConfigurationManager.AppSettings.Get("Database") +
        //                 ";User Id=" + ConfigurationManager.AppSettings.Get("SQLUserID") +
        //                 ";Password=" + ConfigurationManager.AppSettings.Get("SQLUserPassword") + 
        //                 ";";

        static string strConnectionSting = "Server=" + ConfigurationManager.AppSettings.Get("DBServer") +
                            ";Database=" + ConfigurationManager.AppSettings.Get("Database") +
                            ";Trusted_Connection=True;";
        static void Main(string[] args)
        {
            string GeoSireneURL = ConfigurationManager.AppSettings.Get("GeoSireneURL"); //"http://data.cquest.org/geo_sirene/lastss/"; 
            string ArchiveFileDownloadPath = ConfigurationManager.AppSettings.Get("ArchiveFileDownloadPath");//@"D:\Personal\ETL\alacroix\FileList\";
            string ExtractFilePath = ConfigurationManager.AppSettings.Get("ExtractFilePath"); //@"D:\Personal\ETL\alacroix\FileList\";
            // proposition pour gérer les points comme séparateur décimal (alx)                                                                                //
              System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
            

            string html = ReadHtmlContentFromUrl(GeoSireneURL);
            if (html != "error")
            {
                string status = ExecuteTableTruncateSTMT("GeoSirene");
                if (status != "error")
                {
                    string[] FileList = GetAllFileList(html);
                    int cnt = 0;
                    foreach (string FileName in FileList)
                    {
                        if (!String.IsNullOrEmpty(FileName))
                        {
                            if (File.Exists(ArchiveFileDownloadPath + FileName))
                            {
                                File.Delete(ArchiveFileDownloadPath + FileName);
                            }
                            try
                            {
                                using (var client = new WebClient())
                                {
                                    client.DownloadFile(GeoSireneURL + FileName, ArchiveFileDownloadPath + FileName);
                                    Console.Write("File " + FileName + " Hass been Downloaded successfully \n");
                                }

                                string ArchiveFullFilePath = ArchiveFileDownloadPath + FileName;
                                unZipFile(ArchiveFullFilePath, ExtractFilePath);
                                string csvFileName = FileName.Replace(".7z", "");
                                Console.Write("File " + FileName + " successfully extracted to " + csvFileName + " \n");
                                readGeoSireneFile(ExtractFilePath + csvFileName, FileName);
                                Console.Write("File " + csvFileName + " has been loaded into Database \n\n");
                            }
                            catch (Exception ex)
                            {
                                Console.Write("\n\nFollwoing Error has occured.\n" + ex.Message.ToString() + "\nPlease try again later.");
                                var userinput = Console.ReadLine();
                                break;
                            }
                        }
                        cnt++;
                        //if (cnt == 5)
                        //    break;
                    }
                    Console.Write("\n\nAll File has been loaded. Please Hit enter to finish.");
                    var userinput1 = Console.ReadLine();
                }
            }           
        }
        public static string[] GetAllFileList(string html)
        {
            Regex regexFile = new Regex(@"(<a.*?>.*?</a>)", RegexOptions.Singleline);
            MatchCollection matchesFile = regexFile.Matches(html);
            int counter = 0;
            string[] FileList = new string[matchesFile.Count];
            foreach (Match match in matchesFile)
            {
                string value = match.Groups[1].Value;
                string t = Regex.Replace(value, @"\s*<.*?>\s*", "", RegexOptions.Singleline);
                value = t;
                if (value.Contains(".csv.7z"))
                {
                    FileList[counter] = value;
                    counter++;
                }
            }
            return FileList;
        }
        public static string ReadHtmlContentFromUrl(string url)
        {
            string html = string.Empty;
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using (Stream stream = response.GetResponseStream())
                using (StreamReader reader = new StreamReader(stream))
                {
                    html = reader.ReadToEnd();
                }
            }
            catch (Exception ex)
            {
                Console.Write("Error: \n" + ex.Message.ToString() + "\nPlease try again later.");
                html = "error";
                var userinput = Console.ReadLine();
            }
            return html;
        }

        public static void unZipFile(string ArchiveFilePath, string Desnation)
        {
            using (ArchiveFile archiveFile = new ArchiveFile(ArchiveFilePath))
            {
                foreach (Entry entry in archiveFile.Entries)
                {
                    if (File.Exists(Desnation + entry.FileName.ToString()))
                    {
                        File.Delete(Desnation + entry.FileName.ToString());
                    }

                    MemoryStream memoryStream = new MemoryStream();
                    entry.Extract(memoryStream);
                    memoryStream.Position = 0;

                    using (FileStream file = new FileStream(Desnation + entry.FileName.ToString(), FileMode.Create, FileAccess.Write))
                    {
                        memoryStream.WriteTo(file);
                    }

                }
            }
        }

        private static void readGeoSireneFile(string filePath, string fileName)
        {
            System.Data.DataTable GeoSirene = new System.Data.DataTable();

            GeoSirene.Columns.Add("SIREN", typeof(String));
            GeoSirene.Columns.Add("NIC", typeof(String));
            GeoSirene.Columns.Add("L1_NORMALISEE", typeof(String));
            GeoSirene.Columns.Add("L2_NORMALISEE", typeof(String));
            GeoSirene.Columns.Add("L3_NORMALISEE", typeof(String));
            GeoSirene.Columns.Add("L4_NORMALISEE", typeof(String));
            GeoSirene.Columns.Add("L5_NORMALISEE", typeof(String));
            GeoSirene.Columns.Add("L6_NORMALISEE", typeof(String));
            GeoSirene.Columns.Add("L7_NORMALISEE", typeof(String));
            GeoSirene.Columns.Add("L1_DECLAREE", typeof(String));
            GeoSirene.Columns.Add("L2_DECLAREE", typeof(String));
            GeoSirene.Columns.Add("L3_DECLAREE", typeof(String));
            GeoSirene.Columns.Add("L4_DECLAREE", typeof(String));
            GeoSirene.Columns.Add("L5_DECLAREE", typeof(String));
            GeoSirene.Columns.Add("L6_DECLAREE", typeof(String));
            GeoSirene.Columns.Add("L7_DECLAREE", typeof(String));
            GeoSirene.Columns.Add("NUMVOIE", typeof(String));
            GeoSirene.Columns.Add("INDREP", typeof(String));
            GeoSirene.Columns.Add("TYPVOIE", typeof(String));
            GeoSirene.Columns.Add("LIBVOIE", typeof(String));
            GeoSirene.Columns.Add("CODPOS", typeof(String)); 
            GeoSirene.Columns.Add("CEDEX", typeof(Int32));
            GeoSirene.Columns.Add("RPET", typeof(String));
            GeoSirene.Columns.Add("LIBREG", typeof(String));
            GeoSirene.Columns.Add("DEPET", typeof(String));
            GeoSirene.Columns.Add("ARRONET", typeof(Int32));
            GeoSirene.Columns.Add("CTONET", typeof(String));
            GeoSirene.Columns.Add("COMET", typeof(String));
            GeoSirene.Columns.Add("LIBCOM", typeof(String));
            GeoSirene.Columns.Add("DU", typeof(String));
            GeoSirene.Columns.Add("TU", typeof(Int32));
            GeoSirene.Columns.Add("UU", typeof(String));
            GeoSirene.Columns.Add("EPCI", typeof(Int64));
            GeoSirene.Columns.Add("TCD", typeof(String));
            GeoSirene.Columns.Add("ZEMET", typeof(String));
            GeoSirene.Columns.Add("SIEGE", typeof(Int32));
            GeoSirene.Columns.Add("ENSEIGNE", typeof(String));
            GeoSirene.Columns.Add("IND_PUBLIPO", typeof(Int32));
            GeoSirene.Columns.Add("DIFFCOM", typeof(String));
            GeoSirene.Columns.Add("AMINTRET", typeof(Int32));
            GeoSirene.Columns.Add("NATETAB", typeof(Int32));
            GeoSirene.Columns.Add("LIBNATETAB", typeof(String));
            GeoSirene.Columns.Add("APET700", typeof(String));
            GeoSirene.Columns.Add("LIBAPET", typeof(String));
            GeoSirene.Columns.Add("DAPET", typeof(Int32));
            GeoSirene.Columns.Add("TEFET", typeof(String));
            GeoSirene.Columns.Add("LIBTEFET", typeof(String));
            GeoSirene.Columns.Add("EFETCENT", typeof(String));
            GeoSirene.Columns.Add("DEFET", typeof(Int32));
            GeoSirene.Columns.Add("ORIGINE", typeof(String));
            GeoSirene.Columns.Add("DCRET", typeof(Int32));
            GeoSirene.Columns.Add("DDEBACT", typeof(Int32));
            GeoSirene.Columns.Add("ACTIVNAT", typeof(String));
            GeoSirene.Columns.Add("LIEUACT", typeof(String));
            GeoSirene.Columns.Add("ACTISURF", typeof(String));
            GeoSirene.Columns.Add("SAISONAT", typeof(String));
            GeoSirene.Columns.Add("MODET", typeof(String));
            GeoSirene.Columns.Add("PRODET", typeof(String));
            GeoSirene.Columns.Add("PRODPART", typeof(String));
            GeoSirene.Columns.Add("AUXILT", typeof(Int32));
            GeoSirene.Columns.Add("NOMEN_LONG", typeof(String));
            GeoSirene.Columns.Add("SIGLE", typeof(String));
            GeoSirene.Columns.Add("NOM", typeof(String));
            GeoSirene.Columns.Add("PRENOM", typeof(String));
            GeoSirene.Columns.Add("CIVILITE", typeof(Int32));
            GeoSirene.Columns.Add("RNA", typeof(String));
            GeoSirene.Columns.Add("NICSIEGE", typeof(String));
            GeoSirene.Columns.Add("RPEN", typeof(String));
            GeoSirene.Columns.Add("DEPCOMEN", typeof(String));
            GeoSirene.Columns.Add("ADR_MAIL", typeof(String));
            GeoSirene.Columns.Add("NJ", typeof(Int32));
            GeoSirene.Columns.Add("LIBNJ", typeof(String));
            GeoSirene.Columns.Add("APEN700", typeof(String));
            GeoSirene.Columns.Add("LIBAPEN", typeof(String));
            GeoSirene.Columns.Add("DAPEN", typeof(Int32));
            GeoSirene.Columns.Add("APRM", typeof(String));
            GeoSirene.Columns.Add("ESS", typeof(String));
            GeoSirene.Columns.Add("DATEESS", typeof(Int32));
            GeoSirene.Columns.Add("TEFEN", typeof(String));
            GeoSirene.Columns.Add("LIBTEFEN", typeof(String));
            GeoSirene.Columns.Add("EFENCENT", typeof(String));
            GeoSirene.Columns.Add("DEFEN", typeof(Int32));
            GeoSirene.Columns.Add("CATEGORIE", typeof(String));
            GeoSirene.Columns.Add("DCREN", typeof(Int32));
            GeoSirene.Columns.Add("AMINTREN", typeof(Int32));
            GeoSirene.Columns.Add("MONOACT", typeof(Int32));
            GeoSirene.Columns.Add("MODEN", typeof(String));
            GeoSirene.Columns.Add("PRODEN", typeof(String));
            GeoSirene.Columns.Add("ESAANN", typeof(String));
            GeoSirene.Columns.Add("TCA", typeof(String));
            GeoSirene.Columns.Add("ESAAPEN", typeof(String));
            GeoSirene.Columns.Add("ESASEC1N", typeof(String));
            GeoSirene.Columns.Add("ESASEC2N", typeof(String));
            GeoSirene.Columns.Add("ESASEC3N", typeof(String));
            GeoSirene.Columns.Add("ESASEC4N", typeof(String));
            GeoSirene.Columns.Add("VMAJ", typeof(String));
            GeoSirene.Columns.Add("VMAJ1", typeof(String));
            GeoSirene.Columns.Add("VMAJ2", typeof(String));
            GeoSirene.Columns.Add("VMAJ3", typeof(String));
            GeoSirene.Columns.Add("DATEMAJ", typeof(DateTime));
            GeoSirene.Columns.Add("longitude", typeof(float));
            GeoSirene.Columns.Add("latitude", typeof(float));
            GeoSirene.Columns.Add("geo_score", typeof(float));
            GeoSirene.Columns.Add("geo_type", typeof(String));
            GeoSirene.Columns.Add("geo_adresse", typeof(String));
            GeoSirene.Columns.Add("geo_id", typeof(String));
            GeoSirene.Columns.Add("geo_ligne", typeof(String));
            GeoSirene.Columns.Add("geo_l4", typeof(String));
            GeoSirene.Columns.Add("geo_l5", typeof(String));
            GeoSirene.Columns.Add("SourceFileName", typeof(String));

            int intRowCounter = 0;
            string strLine = "";
            int intSkipHeader = 0;
            string[] strLineFields = null;
            string[] strFields = "SIREN,NIC,L1_NORMALISEE,L2_NORMALISEE,L3_NORMALISEE,L4_NORMALISEE,L5_NORMALISEE,L6_NORMALISEE,L7_NORMALISEE,L1_DECLAREE,L2_DECLAREE,L3_DECLAREE,L4_DECLAREE,L5_DECLAREE,L6_DECLAREE,L7_DECLAREE,NUMVOIE,INDREP,TYPVOIE,LIBVOIE,CODPOS,CEDEX,RPET,LIBREG,DEPET,ARRONET,CTONET,COMET,LIBCOM,DU,TU,UU,EPCI,TCD,ZEMET,SIEGE,ENSEIGNE,IND_PUBLIPO,DIFFCOM,AMINTRET,NATETAB,LIBNATETAB,APET700,LIBAPET,DAPET,TEFET,LIBTEFET,EFETCENT,DEFET,ORIGINE,DCRET,DDEBACT,ACTIVNAT,LIEUACT,ACTISURF,SAISONAT,MODET,PRODET,PRODPART,AUXILT,NOMEN_LONG,SIGLE,NOM,PRENOM,CIVILITE,RNA,NICSIEGE,RPEN,DEPCOMEN,ADR_MAIL,NJ,LIBNJ,APEN700,LIBAPEN,DAPEN,APRM,ESS,DATEESS,TEFEN,LIBTEFEN,EFENCENT,DEFEN,CATEGORIE,DCREN,AMINTREN,MONOACT,MODEN,PRODEN,ESAANN,TCA,ESAAPEN,ESASEC1N,ESASEC2N,ESASEC3N,ESASEC4N,VMAJ,VMAJ1,VMAJ2,VMAJ3,DATEMAJ,longitude,latitude,geo_score,geo_type,geo_adresse,geo_id,geo_ligne,geo_l4,geo_l5,SourceFileName".Split(',');
            
            StreamReader objStreamReader = null;
            try
            {
                objStreamReader = new StreamReader(filePath);
                while (!objStreamReader.EndOfStream)
                {
                    strLine = objStreamReader.ReadLine();
                    if (intSkipHeader == 0)
                    {
                        intSkipHeader = 1;
                        continue;
                    }
                    strLineFields = SplitCSV(strLine);
                   
                   
                   DataRow GeoSirene_Row = GeoSirene.NewRow();
                    for (int i = 0; i < strFields.Length; i++)
                    {
                        if (strFields[i] == "SourceFileName")
                        {
                            GeoSirene_Row[strFields[i]] = fileName;
                        }
                        else
                        {
                            if (strLineFields[i].Trim() == "")
                            {
                                GeoSirene_Row[strFields[i]] = DBNull.Value;
                            }
                            else
                            {
                                GeoSirene_Row[strFields[i]] = strLineFields[i].Trim();
                            }

                        }
                         
                    }

                    GeoSirene.Rows.Add(GeoSirene_Row);
                    intRowCounter++;

                    if (intRowCounter >= 10000)
                    {

                        LoadDataIntoDB(GeoSirene);
                        intRowCounter = 0;
                        GeoSirene.Rows.Clear();
                        GC.Collect();
                    }

                }
                if (intRowCounter >= 0)
                {

                    LoadDataIntoDB(GeoSirene);
                    intRowCounter = 0;
                    GeoSirene.Rows.Clear();
                }
                objStreamReader.Close();

            }
            catch (Exception ex)
            {
                objStreamReader.Close();
                throw ex;
            }
        }

   private static string[] SplitCSV(string input)
   {
       Regex csvSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
       List<string> list = new List<string>();
       string curr = null;
       foreach (Match match in csvSplit.Matches(input))
       {
           curr = match.Value;
           if (0 == curr.Length)
           {
               list.Add("");
           }

           list.Add(curr.TrimStart(','));
       }

       return list.ToArray<string>();
   }
        private static string ExecuteTableTruncateSTMT(string TableName){
            string queryString = "TRUNCATE TABLE " + TableName;
            try
            {
                using (SqlConnection connection = new SqlConnection(strConnectionSting))
                {
                    SqlCommand command = new SqlCommand(queryString, connection);
                    connection.Open();
                    command.ExecuteNonQuery();
                }
                queryString = "Ok";
            }
            catch(Exception ex)
            {
                Console.Write("Error: \n" + ex.Message.ToString() + "\nPlease try again later.");
                queryString = "error";
                var userinput = Console.ReadLine();
            }

            return queryString;
        }
        
        private static void ExecuteStroreProcedure(){
            using (var conn = new SqlConnection(strConnectionSting))
            using (var command = new SqlCommand("usp_LoadGeoSirene", conn)
            {
                CommandType = CommandType.StoredProcedure
            })
            {
                command.CommandTimeout = 1000;
                conn.Open();
                command.ExecuteNonQuery();
            }
   }
        private static void LoadDataIntoDB(DataTable GeoSirene)
        {
            try
            {
                using (SqlConnection con = new SqlConnection(strConnectionSting))
                {
                    con.Open();
                    DataRow[] rowArray = GeoSirene.Select();

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con))
                    {
                        bulkCopy.DestinationTableName = "GeoSirene";
                        bulkCopy.BulkCopyTimeout = 3600;
                        try
                        {
                            bulkCopy.ColumnMappings.Add("SIREN", "SIREN");
                            bulkCopy.ColumnMappings.Add("NIC", "NIC");
                            bulkCopy.ColumnMappings.Add("L1_NORMALISEE", "L1_NORMALISEE");
                            bulkCopy.ColumnMappings.Add("L2_NORMALISEE", "L2_NORMALISEE");
                            bulkCopy.ColumnMappings.Add("L3_NORMALISEE", "L3_NORMALISEE");
                            bulkCopy.ColumnMappings.Add("L4_NORMALISEE", "L4_NORMALISEE");
                            bulkCopy.ColumnMappings.Add("L5_NORMALISEE", "L5_NORMALISEE");
                            bulkCopy.ColumnMappings.Add("L6_NORMALISEE", "L6_NORMALISEE");
                            bulkCopy.ColumnMappings.Add("L7_NORMALISEE", "L7_NORMALISEE");
                            bulkCopy.ColumnMappings.Add("L1_DECLAREE", "L1_DECLAREE");
                            bulkCopy.ColumnMappings.Add("L2_DECLAREE", "L2_DECLAREE");
                            bulkCopy.ColumnMappings.Add("L3_DECLAREE", "L3_DECLAREE");
                            bulkCopy.ColumnMappings.Add("L4_DECLAREE", "L4_DECLAREE");
                            bulkCopy.ColumnMappings.Add("L5_DECLAREE", "L5_DECLAREE");
                            bulkCopy.ColumnMappings.Add("L6_DECLAREE", "L6_DECLAREE");
                            bulkCopy.ColumnMappings.Add("L7_DECLAREE", "L7_DECLAREE");
                            bulkCopy.ColumnMappings.Add("NUMVOIE", "NUMVOIE");
                            bulkCopy.ColumnMappings.Add("INDREP", "INDREP");
                            bulkCopy.ColumnMappings.Add("TYPVOIE", "TYPVOIE");
                            bulkCopy.ColumnMappings.Add("LIBVOIE", "LIBVOIE");
                            bulkCopy.ColumnMappings.Add("CODPOS", "CODPOS");
                            bulkCopy.ColumnMappings.Add("CEDEX", "CEDEX");
                            bulkCopy.ColumnMappings.Add("RPET", "RPET");
                            bulkCopy.ColumnMappings.Add("LIBREG", "LIBREG");
                            bulkCopy.ColumnMappings.Add("DEPET", "DEPET");
                            bulkCopy.ColumnMappings.Add("ARRONET", "ARRONET");
                            bulkCopy.ColumnMappings.Add("CTONET", "CTONET");
                            bulkCopy.ColumnMappings.Add("COMET", "COMET");
                            bulkCopy.ColumnMappings.Add("LIBCOM", "LIBCOM");
                            bulkCopy.ColumnMappings.Add("DU", "DU");
                            bulkCopy.ColumnMappings.Add("TU", "TU");
                            bulkCopy.ColumnMappings.Add("UU", "UU");
                            bulkCopy.ColumnMappings.Add("EPCI", "EPCI");
                            bulkCopy.ColumnMappings.Add("TCD", "TCD");
                            bulkCopy.ColumnMappings.Add("ZEMET", "ZEMET");
                            bulkCopy.ColumnMappings.Add("SIEGE", "SIEGE");
                            bulkCopy.ColumnMappings.Add("ENSEIGNE", "ENSEIGNE");
                            bulkCopy.ColumnMappings.Add("IND_PUBLIPO", "IND_PUBLIPO");
                            bulkCopy.ColumnMappings.Add("DIFFCOM", "DIFFCOM");
                            bulkCopy.ColumnMappings.Add("AMINTRET", "AMINTRET");
                            bulkCopy.ColumnMappings.Add("NATETAB", "NATETAB");
                            bulkCopy.ColumnMappings.Add("LIBNATETAB", "LIBNATETAB");
                            bulkCopy.ColumnMappings.Add("APET700", "APET700");
                            bulkCopy.ColumnMappings.Add("LIBAPET", "LIBAPET");
                            bulkCopy.ColumnMappings.Add("DAPET", "DAPET");
                            bulkCopy.ColumnMappings.Add("TEFET", "TEFET");
                            bulkCopy.ColumnMappings.Add("LIBTEFET", "LIBTEFET");
                            bulkCopy.ColumnMappings.Add("EFETCENT", "EFETCENT");
                            bulkCopy.ColumnMappings.Add("DEFET", "DEFET");
                            bulkCopy.ColumnMappings.Add("ORIGINE", "ORIGINE");
                            bulkCopy.ColumnMappings.Add("DCRET", "DCRET");
                            bulkCopy.ColumnMappings.Add("DDEBACT", "DDEBACT");
                            bulkCopy.ColumnMappings.Add("ACTIVNAT", "ACTIVNAT");
                            bulkCopy.ColumnMappings.Add("LIEUACT", "LIEUACT");
                            bulkCopy.ColumnMappings.Add("ACTISURF", "ACTISURF");
                            bulkCopy.ColumnMappings.Add("SAISONAT", "SAISONAT");
                            bulkCopy.ColumnMappings.Add("MODET", "MODET");
                            bulkCopy.ColumnMappings.Add("PRODET", "PRODET");
                            bulkCopy.ColumnMappings.Add("PRODPART", "PRODPART");
                            bulkCopy.ColumnMappings.Add("AUXILT", "AUXILT");
                            bulkCopy.ColumnMappings.Add("NOMEN_LONG", "NOMEN_LONG");
                            bulkCopy.ColumnMappings.Add("SIGLE", "SIGLE");
                            bulkCopy.ColumnMappings.Add("NOM", "NOM");
                            bulkCopy.ColumnMappings.Add("PRENOM", "PRENOM");
                            bulkCopy.ColumnMappings.Add("CIVILITE", "CIVILITE");
                            bulkCopy.ColumnMappings.Add("RNA", "RNA");
                            bulkCopy.ColumnMappings.Add("NICSIEGE", "NICSIEGE");
                            bulkCopy.ColumnMappings.Add("RPEN", "RPEN");
                            bulkCopy.ColumnMappings.Add("DEPCOMEN", "DEPCOMEN");
                            bulkCopy.ColumnMappings.Add("ADR_MAIL", "ADR_MAIL");
                            bulkCopy.ColumnMappings.Add("NJ", "NJ");
                            bulkCopy.ColumnMappings.Add("LIBNJ", "LIBNJ");
                            bulkCopy.ColumnMappings.Add("APEN700", "APEN700");
                            bulkCopy.ColumnMappings.Add("LIBAPEN", "LIBAPEN");
                            bulkCopy.ColumnMappings.Add("DAPEN", "DAPEN");
                            bulkCopy.ColumnMappings.Add("APRM", "APRM");
                            bulkCopy.ColumnMappings.Add("ESS", "ESS");
                            bulkCopy.ColumnMappings.Add("DATEESS", "DATEESS");
                            bulkCopy.ColumnMappings.Add("TEFEN", "TEFEN");
                            bulkCopy.ColumnMappings.Add("LIBTEFEN", "LIBTEFEN");
                            bulkCopy.ColumnMappings.Add("EFENCENT", "EFENCENT");
                            bulkCopy.ColumnMappings.Add("DEFEN", "DEFEN");
                            bulkCopy.ColumnMappings.Add("CATEGORIE", "CATEGORIE");
                            bulkCopy.ColumnMappings.Add("DCREN", "DCREN");
                            bulkCopy.ColumnMappings.Add("AMINTREN", "AMINTREN");
                            bulkCopy.ColumnMappings.Add("MONOACT", "MONOACT");
                            bulkCopy.ColumnMappings.Add("MODEN", "MODEN");
                            bulkCopy.ColumnMappings.Add("PRODEN", "PRODEN");
                            bulkCopy.ColumnMappings.Add("ESAANN", "ESAANN");
                            bulkCopy.ColumnMappings.Add("TCA", "TCA");
                            bulkCopy.ColumnMappings.Add("ESAAPEN", "ESAAPEN");
                            bulkCopy.ColumnMappings.Add("ESASEC1N", "ESASEC1N");
                            bulkCopy.ColumnMappings.Add("ESASEC2N", "ESASEC2N");
                            bulkCopy.ColumnMappings.Add("ESASEC3N", "ESASEC3N");
                            bulkCopy.ColumnMappings.Add("ESASEC4N", "ESASEC4N");
                            bulkCopy.ColumnMappings.Add("VMAJ", "VMAJ");
                            bulkCopy.ColumnMappings.Add("VMAJ1", "VMAJ1");
                            bulkCopy.ColumnMappings.Add("VMAJ2", "VMAJ2");
                            bulkCopy.ColumnMappings.Add("VMAJ3", "VMAJ3");
                            bulkCopy.ColumnMappings.Add("DATEMAJ", "DATEMAJ");
                            bulkCopy.ColumnMappings.Add("longitude", "longitude");
                            bulkCopy.ColumnMappings.Add("latitude", "latitude");
                            bulkCopy.ColumnMappings.Add("geo_score", "geo_score");
                            bulkCopy.ColumnMappings.Add("geo_type", "geo_type");
                            bulkCopy.ColumnMappings.Add("geo_adresse", "geo_adresse");
                            bulkCopy.ColumnMappings.Add("geo_id", "geo_id");
                            bulkCopy.ColumnMappings.Add("geo_ligne", "geo_ligne");
                            bulkCopy.ColumnMappings.Add("geo_l4", "geo_l4");
                            bulkCopy.ColumnMappings.Add("geo_l5", "geo_l5");
                            bulkCopy.ColumnMappings.Add("SourceFileName", "SourceFileName");

                            bulkCopy.WriteToServer(rowArray);
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }
                }

            }
            catch (Exception ex)
            {

                throw ex;
            }
            
        }

    }

}
