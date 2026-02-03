package com.dataflow.dataloaders.jobconfigs;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;

@Setter
@Getter
@Component
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InputField {
   private String name;
   private String type;
   private String fieldLength;
   private Object value;
   private LinkedHashMap<String, LinkedHashMap<PROCESSOR_ENUM,Object>> processors;
   private String udtField;
   private ROW_CONDENSER rowCondenser = ROW_CONDENSER.FIRST; //default is to take first value encountered
   private Integer columnIndex;
   private String expectedValue; //used in CassandraFilterFieldProcessor

   //mongo writer field settings
   private String documentPath = "";
   private boolean jsonField = false;
   private boolean keepJsonOuterObject = false;

   public enum ROW_CONDENSER {
      FIRST, LAST, CONCATENATE
   }

   @Override
   public String toString() {
       StringBuilder sb = new StringBuilder("{");
       if (name != null) sb.append("name=").append(name).append(", ");
       if (type != null) sb.append("type=").append(type).append(", ");
       if (processors != null) sb.append("processors=").append(processors).append(", ");
       if (rowCondenser != null) sb.append("rowCondenser=").append(rowCondenser).append(", ");
       if (sb.length() > 1) sb.setLength(sb.length() - 2);
       sb.append("}");
       return sb.toString();
   }

}
