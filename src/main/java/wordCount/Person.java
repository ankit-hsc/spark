package wordCount;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

	   @JsonIgnoreProperties
		public class Person implements Serializable{
			@JsonProperty(value="name")
			private String name;
			@JsonProperty(value="age")
			private int age;
			public String getName() {
				return name;
			}
			public void setName(String name) {
				this.name = name;
			}
			public int getAge() {
				return age;
			}
			public void setAge(int age) {
				this.age = age;
			}
}
