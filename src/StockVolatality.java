
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class StockVolatality {
	
	/**
	 * @author hduser
	 * seperate the files into lines and lines into token
	 * input: <key, value>, key: line number, value: line
	 * output: <key, value>, key: each word, value: number of occurence 
	 * e.g. input <line1, hello a bye a>
	 * 		output<hello, 1>,<a, 1>, <bye, 1>,<a, 1>
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text key1 = new Text();//set as the column of A or row of B.
		private Text value1 = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context){
			try{
				String line = value.toString();
				String header = "Date,Open,High,Low,Close,Volume,Adj Close";
				String DateAlongWithPrice =null;
				FileSplit fileSplit = (FileSplit)context.getInputSplit();
				String filename = fileSplit.getPath().getName();
				key1.set(filename.toString());
				if(line.equals(header)){
					//do nothing
				}
				else{
					String part[] = null;
					part = line.split(",");
					DateAlongWithPrice = part[0]+ " , " +part[6];
					//Stores 2014-11-2012,33.43
					value1.set(DateAlongWithPrice.toString());
					context.write(key1, value1);
				}

			} // based on space
			catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
		}
	
	//Second Mapper starts here
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		private Text keyOfMap1 = new Text();//set as the column of A or row of B.
		private Text valueOfMap1 = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context){
			try{
				String line = value.toString();
				//APPL, 0.9999854
				
			
				keyOfMap1.set("Same");
				valueOfMap1.set(line.toString().trim());
				context.write(keyOfMap1, valueOfMap1);
			} // based on space
			catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
		}
		//Second Reducer
	public static class Reduce1 extends Reducer<Text, Text, Text, DoubleWritable> {
		//private IntWritable value11 = new IntWritable();
		//private Text key11 = new Text();set as the column of A or row of B.
		
		//private DoubleWritable value11 = new DoubleWritable();
		ArrayList<Double> allVolatility =new ArrayList<Double>();
		HashMap<Double,ArrayList<String>> stockVola = new HashMap<Double,ArrayList<String>>();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text val: values){
				
				
				//context.write(key, new Text(trimmed.toString()));
			
			//val: APPL.csv,  2.342
				if(val.toString().contains(",")){
				String StockVolatility[] = val.toString().trim().split(",");
				String fullFileName = StockVolatility[0].toString().trim();
				String stockName = fullFileName.replaceAll(".csv", "");
				//stockName: AAPL
				String volatility = StockVolatility[1].toString().trim();
				
				//volatility: 2.345
				double singlevolatility = Double.valueOf(volatility.toString());
				
				if(singlevolatility != 0){
					
				String singlestockname = stockName.toString().trim();
				if(stockVola.containsKey(singlevolatility)){
					ArrayList<String> temp = new ArrayList<String>();
					temp = stockVola.get(singlevolatility);
					temp.add(singlestockname);
					stockVola.put(singlevolatility, temp);
					
				}
				else{
					ArrayList<String> newTemp = new ArrayList<String>();
					newTemp.add(singlestockname);
				stockVola.put(singlevolatility,newTemp);
				
				}
				allVolatility.add(singlevolatility);
				StockVolatility=null;
				stockName = null;
				volatility = null;
				}
			}
				}
			
		  
			Collections.sort(allVolatility);
	
			
			context.write(new Text("The top minimum values"),null);
			
			for(int i=0; i<10; ){
				double currentValue = allVolatility.get(i);
				
				ArrayList<String> stocks = new ArrayList<String>();
				stocks = stockVola.get(currentValue);
				
				if(currentValue!=0){
					for(String aa: stocks){
				context.write(new Text(aa), new DoubleWritable(currentValue));
					i=i+1;
					}
				}
				stocks.clear();
				
			}
			context.write(new Text("The top maximum values"), null);
			
			for(int j=allVolatility.size()-1; j>=allVolatility.size()-10; ){
				ArrayList<String> maxstocks = new ArrayList<String>();
				double currentValue = allVolatility.get(j);
				maxstocks = stockVola.get(currentValue);
				if(currentValue!=0)
					for(String aa: maxstocks){
						context.write(new Text(aa), new DoubleWritable(currentValue));
							j=j-1;
							}
			}
			
		}
		



	}
	
	/**
	 * @author hduser
	 * Count the number of times the given keys occur
	 * input: <key, value>, key = Text, value: number of occurrence
	 * output: <key, value>, key = Text, value = number of occurrence
	 * 
	 * */
	//output: <Year/Month, 31-33.95>
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable>{
		//private IntWritable value11 = new IntWritable();
		//private Text key11 = new Text();set as the column of A or row of B.
		private Text value11 = new Text();
		HashMap<String, String> maxDateOfMonthYear = new HashMap<String,String>();
		HashMap<String, String> minDateOfMonthYear = new HashMap<String,String>();
		HashMap<String,String> allPrices = new HashMap<String,String>();
		HashSet<String> totalMonths = new HashSet<String>();
		ArrayList<Double> monthwiseMean =new ArrayList<Double>();
	
		double sumOfmonthlyReturn = 0.0;
		double mean =0;
		double volatality=0;
		double inner=0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
		/*	int max = 0;
			int min = 99;
			String maxPrice = null;
			String minPrice = null;
			double sum = 0;
			double mean = 0.00;*/
			
			for (Text val: values){
				//val = 2014-11-02|33.43
				String DatePricePart[] = val.toString().split(",");
				
				String fulldate = DatePricePart[0].trim();
				String price = DatePricePart[1].trim();
				//ArrayList<Integer> totalPriceValues = new ArrayList<Integer>();
				
				allPrices.put(fulldate, price);
				//totalPriceValues.add(Integer.valueOf(price.toString()));
				String partsOfDate[] = fulldate.split("-");
				String monthYear = partsOfDate[0]+"-"+partsOfDate[1];
				
				//String monthYear = "2013-11";
				
				totalMonths.add(monthYear.toString());
				
				//monthYear = 2014-11
				
			String day = partsOfDate[2];
				//String day = "22";
				//day=02
				if(!maxDateOfMonthYear.containsKey(monthYear)){
					maxDateOfMonthYear.put(monthYear, day);
				}
				else{
					String currentMaxDate = maxDateOfMonthYear.get(monthYear);
					//currentMaxDate=26
					//day=28
					if(Integer.valueOf(day.toString())> Integer.valueOf(currentMaxDate.toString())){
						maxDateOfMonthYear.remove(monthYear);
						maxDateOfMonthYear.put(monthYear, day);
					}	
				}
				
				
				if(!minDateOfMonthYear.containsKey(monthYear)){
					minDateOfMonthYear.put(monthYear, day);
				}
				else{
					String currentMinDate = minDateOfMonthYear.get(monthYear);
					//currentMaxDate=26
					//day=28
					if(Integer.valueOf(day.toString()) < Integer.valueOf(currentMinDate.toString())){
						minDateOfMonthYear.remove(monthYear);
						minDateOfMonthYear.put(monthYear, day);
					}	
				}
			}	
				//Calculate the MonthlyRateOfReturn
			
		for(Entry<String, String> entry : maxDateOfMonthYear.entrySet()){
			
			String currentmaxmonthYear = entry.getKey();
			//This gets each YEARMONTH
			String currentmaxdate = maxDateOfMonthYear.get(currentmaxmonthYear);
			//This gets maximumdate for the YEARMONTH
			String fullMaxDate = currentmaxmonthYear+"-"+currentmaxdate;
			//got the max date for a given month
			String currentmindate = minDateOfMonthYear.get(currentmaxmonthYear);
			//got the min date for a given month
			String fullMinDate = currentmaxmonthYear+"-"+currentmindate;
			String maxDatePrice = allPrices.get(fullMaxDate);
			String minDatePrice = allPrices.get(fullMinDate);
			double StartMonthPrice = Double.valueOf(minDatePrice.toString());
			double EndMonthPrice = Double.valueOf(maxDatePrice.toString());
			double MonthlyRateOfReturn = ((EndMonthPrice - StartMonthPrice)/StartMonthPrice);
			monthwiseMean.add(MonthlyRateOfReturn);
			sumOfmonthlyReturn = sumOfmonthlyReturn + MonthlyRateOfReturn;
			value11.set("this is monthlyrate  ");
			//context.write(value11, new DoubleWritable(MonthlyRateOfReturn));
			//value11.set(maxDatePrice+"--"+minDatePrice);
			
			//context.write(key,value11);
		}
		double N = totalMonths.size();
		value11.set("this is N  ");
		//context.write(value11, new DoubleWritable(N));
		mean = sumOfmonthlyReturn/N;
		value11.set("This is mean");
		//context.write(value11, new DoubleWritable(mean));
		for(Double a : monthwiseMean){
			inner = inner+ Math.pow((a-mean),2);
			
			
		}
		monthwiseMean.clear();
		if(N==1){
			context.write(key, new DoubleWritable(0));
		}
		else{
		Double inner1 = ((1/(N-1))*inner);
		volatality = Math.sqrt(inner1);
		inner = 0;
		sumOfmonthlyReturn=0;
		maxDateOfMonthYear.clear();
		minDateOfMonthYear.clear();
		totalMonths.clear();
		allPrices.clear();
		monthwiseMean.clear();
		
		context.write(new Text(key+","), new DoubleWritable(volatality));
		}
	}

	}

	
				

	
	
	public static void main(String[] args) {
		String OUTPUT_PATH = "intermediate_output";
		String OUTPUT_PATH1 = "./intermediate_output";
		try {
			long start = new Date().getTime();		
			Job job = Job.getInstance();
			//Path outp = new Path(args[2]);
		     job.setJarByClass(StockVolatality.class);
			
			job.setMapperClass(Map.class);
			
			job.setReducerClass(Reduce.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputDirRecursive(job, true);
			FileInputFormat.addInputPath(job, new Path(args[1]));
			//FileOutputFormat.setOutputPath(job, new Path(args[2]));
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			
			job.setJarByClass(StockVolatality.class);
	
			//System.out.println("Abdul");
			job.waitForCompletion(true);
			//Job2
			
			Job job2 = Job.getInstance();
			job2.setJarByClass(StockVolatality.class);
			job2.setMapperClass(StockVolatality.Map1.class);
			job2.setReducerClass(StockVolatality.Reduce1.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			int NOfReducer2 = 1;
			job2.setNumReduceTasks(NOfReducer2);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			//FileInputFormat.setInputDirRecursive(job2, true);
			
			FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH1));
			// TextOutputFormat.setOutputPath(job2, new Path(args[2]));
			
			 FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			
			
			
			
		boolean status = job2.waitForCompletion(true);
			if (status == true) {
				long end = new Date().getTime();
				System.out.println("\nJob took " + (end-start)/1000 + " seconds\n");
				}
			

		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}

