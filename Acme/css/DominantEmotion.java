import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.nio.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileSystem;
public class DominantEmotion
{	
	
	public static class DominantEmotionMapper extends Mapper<LongWritable, Text, Text, Text>
	{		
		public void map(LongWritable Key, Text value, Context con) throws IOException,InterruptedException
		{	/* for splitting of words and then assigning it to certain variables */
			String[] words =value.toString().split("	");
			String Name = words[0];       
			String Emotion = words[1];
			if(Emotion.contains("happy")||Emotion.contains("delight")||Emotion.contains("great_pleasure")||Emotion.contains("joy") ||Emotion.contains("jubilation")||Emotion.contains("triumph")||Emotion.contains("exultation")||Emotion.contains("rejoicing")||Emotion.contains("happiness")||Emotion.contains("gladness")||Emotion.contains("glee")||Emotion.contains("exhilaration")||Emotion.contains("exuberance")||Emotion.contains("elation")||Emotion.contains("euphoria")||Emotion.contains("bliss")||Emotion.contains("ecstasy")||Emotion.contains("rapture")||Emotion.contains("enjoyment")||Emotion.contains("felicity")||Emotion.contains("jouissance"))
			{
				System.out.println("happy");
				Emotion = "happy";		
		}else if(Emotion.equals("sad")||Emotion.equals("unhappiness")||Emotion.equals("sorrow")||Emotion.equals("dejection")||Emotion.equals("depression")||Emotion.equals("misery")||Emotion.equals("despondency")||Emotion.equals("despair")||Emotion.equals("desolation")||Emotion.equals("wretchedness")||Emotion.equals("gloom")||Emotion.equals("gloominess")||Emotion.equals("dolefulness")||Emotion.equals("melancholy")||Emotion.equals("mournfulness")||Emotion.equals("woe")||Emotion.equals("heartache")||Emotion.equals("grief"))	
			{
				Emotion = "sad";
			}else if( Emotion.equals("surprise")||Emotion.equals("shock")||Emotion.equals("bolt from the blue")||Emotion.equals("bombshell")||Emotion.equals("revelation")||Emotion.equals("rude awakening")||Emotion.equals("eye-opener")||Emotion.equals("wake-up call")||Emotion.equals("astonishment")||Emotion.equals("amazing")||Emotion.equals("wonder")||Emotion.equals("incredulity")||Emotion.equals("bewilderment")||Emotion.equals("stupefaction")||Emotion.equals("disbelief")||Emotion.equals("astonish")||Emotion.equals("amaze")||Emotion.equals("startle")||Emotion.equals("astound")||Emotion.equals("stun")||Emotion.equals("stagger")||Emotion.equals("shock")||Emotion.equals("dumbfound")||Emotion.equals("stupefy")||Emotion.equals("daze")||Emotion.equals("astonished")||Emotion.equals("amazed")||Emotion.equals("astounded")||Emotion.equals("startled")||Emotion.equals("stunned")||Emotion.equals("staggered")||Emotion.equals("nonplussed")||Emotion.equals("shocked")||Emotion.equals("taken aback")||Emotion.equals("stupefied")||Emotion.equals("dumbfounded")||Emotion.equals("dumbstruck")||Emotion.equals("speechless")||Emotion.equals("thunderstruck")||Emotion.equals("confounded")||Emotion.equals("shaken up")||Emotion.equals("unexpected")||Emotion.equals("unforeseen")||Emotion.equals("unpredictable"))
			{
			Emotion="surprise";

			}else if(Emotion.equals("anger")||Emotion.equals("rage")||Emotion.equals("vexation")||Emotion.equals("exasperation")||Emotion.equals("displeasure")||Emotion.equals("crossness")||Emotion.equals("irritation")||Emotion.equals("irritability")||Emotion.equals("indignation")||Emotion.equals("pique")||Emotion.equals("annoyance")||Emotion.equals("fury")||Emotion.equals("wrath")||Emotion.equals("ire")||Emotion.equals("outrage")||Emotion.equals("irascibility")||Emotion.equals("ill temper")||Emotion.equals("aggravation")||Emotion.equals("infuriate")||Emotion.equals("irritate")||Emotion.equals("exasperate")||Emotion.equals("irk")||Emotion.equals("vex")||Emotion.equals("peeve")||Emotion.equals("madden")||Emotion.equals("enrage")||Emotion.equals("incense")||Emotion.equals("annoy"))
			{
			Emotion="anger";
			}else if(Emotion.equals("fear")||Emotion.equals("terror")||Emotion.equals("fright")||Emotion.equals("fearfulness")||Emotion.equals("horror")||Emotion.equals("alarm")||Emotion.equals("panic")||Emotion.equals("agitation")||Emotion.equals("trepidation")||Emotion.equals("dread")||Emotion.equals("consternation")||Emotion.equals("dismay")||Emotion.equals("distress")||Emotion.equals("worry")||Emotion.equals("angst")||Emotion.equals("unease")||Emotion.equals("uneasiness")||Emotion.equals("apprehension")||Emotion.equals("apprehensiveness")||Emotion.equals("nervousness")||Emotion.equals("nerves")||Emotion.equals("perturbation")||Emotion.equals("foreboding")||Emotion.equals("phobia")||Emotion.equals("aversion")||Emotion.equals("antipathy")||Emotion.equals("bugbear")||Emotion.equals("nightmare")||Emotion.equals("horror")||Emotion.equals("terror")||Emotion.equals("anxiety")||Emotion.equals("neurosis"))
			{
				Emotion="fear";
			}else if(Emotion.equals("disgust")||Emotion.equals("revulsion")||Emotion.equals("repugnance")||Emotion.equals("aversion")||Emotion.equals("distaste")||Emotion.equals("nausea")||Emotion.equals("abhorrence")||Emotion.equals("loathing")||Emotion.equals("detestation")||Emotion.equals("odium")||Emotion.equals("horror")||Emotion.equals("contempt")||Emotion.equals("outrage")||Emotion.equals("revolt")||Emotion.equals("repel")||Emotion.equals("repulse")||Emotion.equals("sicken")||Emotion.equals("nauseate"))
				{
					Emotion="disgust";
			}
			else{
				Emotion="unknown";
			}
			System.out.println(Name+"	"+Emotion);
			con.write(new Text(Name),new Text(Emotion));
	}
}
	public static class DominantEmotionReducer extends Reducer<Text,Text,Text,Text>
		{
		public void reduce(Text Name, Iterable<Text> Emotion,Context con) throws IOException, InterruptedException
		{
			
			int happy1=0;
                           int sad1=0;
                           int surprise1=0;
                           int disgust1=0;
                           int fear1=0;
                           int anger1=0;
			  int unknown1=0;
			   for(Text emotion : Emotion){
			   if(emotion.toString().equals("happy"))
			{
				++happy1;		
		}else if(emotion.toString().equals("sad"))	
			{
				++sad1;
			}else if(emotion.toString().equals("surprise"))
			{
			++surprise1;
			}else if(emotion.toString().equals( "anger"))
			{
			++anger1;
			}else if(emotion.toString().equals( "fear"))
			{
				++fear1;
			}else if(emotion.toString().equals("disgust"))
				{++disgust1;
			}
			else if(Emotion.equals("unknown")){
			
			}
		}
				String string1 = String.format("%d\t%d\t%d\t%d\t%d\t%d\t%d",happy1,sad1,surprise1,anger1,fear1,disgust1,unknown1);

			con.write(new Text(Name),new Text(string1));
		}			
	}
	public static void main(String args[]) throws Exception
	{	

		Configuration c = new Configuration();
        FileSystem hdfs =FileSystem.get(new URI("hdfs://hadoop1:9000"), c);
        Job job = Job.getInstance(c, "Map Side Join");

		Path p1 = new Path(args[0]);
		Path p2 = new Path(args[1]);
		Job j = Job.getInstance(c,"Dominant Emotion");
		j.setJarByClass(DominantEmotion.class);
		j.setMapperClass(DominantEmotionMapper.class);
		j.setReducerClass(DominantEmotionReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j,p1);
		FileOutputFormat.setOutputPath(j,p2);


		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}



