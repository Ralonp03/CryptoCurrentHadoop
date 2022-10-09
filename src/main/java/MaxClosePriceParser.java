
import org.apache.hadoop.io.Text;

//Parser for the Mapper
public class MaxClosePriceParser{

  //the value of close price	
  private double valueClose;
  //use only if the line is corrupted
  private boolean estado = true;	
  //the king of crypto moneda, the key
  private String moneda;
  public void parse(String record) {
    
    //variables aux
    int columna = 0;
    String valueAux = "";
    String monedaAux = "";
    
    try{
    //for reading the values acording with the input format
    for(int i = 0;i<record.length();i++){
    	if(record.charAt(i) != ','){
     	if(columna ==2){
    	monedaAux = monedaAux + record.charAt(i);
    	}
    	if(columna == 8){
    	valueAux = valueAux + record.charAt(i);
    	}
    	}if(record.charAt(i) == ','){
    	columna++;
    	}
    }	
    	//assign to thea atributes
    	valueClose = Double.parseDouble(valueAux);
	moneda = monedaAux;
    
    }catch(Exception e){
    	//if the line is worng, we discard it
    	estado = false;
    }
    
  }
  
  //use only if the line is corrrupted
  public boolean valorCorrecto(){
  	return this.estado;
  }
  //parse the Text type
  public void parse(Text record) {
    parse(record.toString());
  }
  //return the name of the moneda	
  public String getMonedaName() {
    return this.moneda;
  }

  //return the value
  public double getValue() {
    return this.valueClose;
  }
}

