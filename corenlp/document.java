import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class document 
{
	private String doc_id;
	private String article_body;
	private String pubclication_date;
	private String mongo_id;
	
	public String getDoc_id() 
	{
		return doc_id;
	}
	public void setDoc_id(String doc_id) 
	{
		this.doc_id = doc_id;
	}
	public String getArticle_body() 
	{
		return article_body;
	}
	public void setArticle_body(String article_body) 
	{
		this.article_body = article_body;
	}
	public String getPubclication_date() 
	{
		return pubclication_date;
	}
	public void setPubclication_date(String pubclication_date) 
	{
		this.pubclication_date = pubclication_date;
	}
	public String getMongo_id() 
	{
		return mongo_id;
	}
	public void setMongo_id(String mongo_id) 
	{
		this.mongo_id = mongo_id;
	}
	
	document(String json_document)
	{
		try 
		{
			JSONParser json_parser = new JSONParser();
			JSONObject json = (JSONObject) json_parser.parse(json_document);
			String doc_id= (String) json.get("doc_id");
            String article_body=(String) json.get("article_body");
            JSONObject json_date = (JSONObject) json.get("publication_date");
            String pub_date = String.valueOf(json_date.get("$date"));
            String mongo_id_json_str=doc_id;
            //JSONObject mongo_id_json_obj= (JSONObject) json_parser.parse(mongo_id_json_str);
            String mongo_id= doc_id;
            
            setDoc_id(doc_id);
            setArticle_body(article_body);
            setPubclication_date(pub_date);
            setMongo_id(mongo_id);
		} 
		catch (ParseException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

