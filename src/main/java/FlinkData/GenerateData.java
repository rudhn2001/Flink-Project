package FlinkData;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;





import java.time.ZonedDateTime;

public class GenerateData {
    private String fname;
    private String lname;
    private String address;
    private String email;
    private String contact;

    public String getFname() {
        return fname;
    }

    public void setFname(String fname) {
        this.fname = fname;
    }

    public String getLname() {
        return lname;
    }

    public void setLname(String lname) {
        this.lname = lname;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getContact() {
        return contact;
    }

    public void setContact(String contact) {
        this.contact = contact;
    }

    // public String toJson() {
    //     ObjectMapper objectMapper = new ObjectMapper();
    //     try {
    //         return objectMapper.writeValueAsString(this);
    //     } catch (JsonProcessingException e) {
    //         e.printStackTrace();
    //         return null;
    //     }
    // }

    @Override
    public String toString() {
        return "GenerateData{" +
                "fname='" + fname + '\'' +
                ", lname='" + lname + '\'' +
                ", address='" + address + '\'' +
                ", email='" + email + '\'' +
                ", contact='" + contact + '\'' +
                '}';
    }
}
