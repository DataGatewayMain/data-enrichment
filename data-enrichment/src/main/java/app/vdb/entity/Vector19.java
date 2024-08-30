package app.vdb.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

import java.io.Serializable;

import jakarta.persistence.Access;
import jakarta.persistence.AccessType;
@Entity
@Table(name = "vector19")
@Access(AccessType.FIELD)
public class Vector19 implements Serializable {

    private static final long serialVersionUID = 1L;
	
    public Vector19() {
		
	}
	
	 	@Id
	    @Column(name = "id")
	    private Long id;
	 	
		public Boolean getemailStatus() {
	        return emailStatus;
	    }

	    public void setemailStatus(Boolean emailStatus) {
	        this.emailStatus = emailStatus;
	    }
	    
		public Boolean getcompanyStatus() {
	        return companyStatus;
	    }

	    public void setcompanyStatus(Boolean companyStatus) {
	        this.companyStatus = companyStatus;
	    }
	 	
		public Boolean getjobTitleStatus() {
	        return jobTitleStatus;
	    }

	    public void setjobTitleStatus(Boolean jobTitleStatus) {
	        this.jobTitleStatus = jobTitleStatus;
	    }
	    
	    public Boolean getSaved() {
	        return saved;
	    }

	    public void setSaved(Boolean saved) {
	        this.saved = saved;
	    }
	    
	 	@Transient
	    private Boolean updated;
	 	
	 	@Transient
	 	private Boolean emailStatus;

	 	@Transient
	 	private Boolean companyStatus;

	 	@Transient
	 	private Boolean jobTitleStatus;
	 	
	 	@Transient
	    private Boolean saved;

	    @Column(name = "pid")
	    private String pid;

	    @Column(name = "first_name")
	    private String first_name;

	    @Column(name = "last_name")
	    private String last_name;

	    @Column(name = "email_address")
	    private String email_address;

	    @Column(name = "company_name")
	    private String company_name;

	    @Column(name = "company_domain")
	    private String company_domain;

	    @Column(name = "job_title")
	    private String job_title;

	    @Column(name = "job_function")
	    private String job_function;

	    @Column(name = "job_level")
	    private String job_level;

	    @Column(name = "company_address")
	    private String company_address;

	    @Column(name = "city")
	    private String city;

	    @Column(name = "state")
	    private String state;

	    @Column(name = "zip_code")
	    private String zip_code;

	    @Column(name = "country")
	    private String country;

	    @Column(name = "telephone_number")
	    private String telephone_number;

	    @Column(name = "employee_size")
	    private String employee_size;

	    @Column(name = "industry")
	    private String industry;

	    @Column(name = "revenue")
	    private String revenue;

	    @Column(name = "sic")
	    private String sic;

	    @Column(name = "naic")
	    private String naic;

	    @Column(name = "company_link")
	    private String company_link;

	    @Column(name = "prospect_link")
	    private String prospect_link;

	    @Column(name = "email_validation")
	    private String email_validation;

	    @Column(name = "headquarter_address")
	    private String headquarter_address;

	    @Column(name = "head_city")
	    private String head_city;

	    @Column(name = "head_state")
	    private String head_state;

	    @Column(name = "campaign_id")
	    private String campaign_id;

	    @Column(name = "api")
	    private String api;

	    @Column(name = "region")
	    private String region;



		public Vector19(Long id, String pid, String first_name, String last_name, String email_address,
				String company_name, String company_domain, String job_title, String job_function, String job_level,
				String company_address, String city, String state, String zip_code, String country,
				String telephone_number, String employee_size, String industry, String revenue, String sic, String naic,
				String company_link, String prospect_link, String email_validation, String headquarter_address,
				String head_city, String head_state, String campaign_id, String api, String region) {
			super();
			this.id = id;
			this.pid = pid;
			this.first_name = first_name;
			this.last_name = last_name;
			this.email_address = email_address;
			this.company_name = company_name;
			this.company_domain = company_domain;
			this.job_title = job_title;
			this.job_function = job_function;
			this.job_level = job_level;
			this.company_address = company_address;
			this.city = city;
			this.state = state;
			this.zip_code = zip_code;
			this.country = country;
			this.telephone_number = telephone_number;
			this.employee_size = employee_size;
			this.industry = industry;
			this.revenue = revenue;
			this.sic = sic;
			this.naic = naic;
			this.company_link = company_link;
			this.prospect_link = prospect_link;
			this.email_validation = email_validation;
			this.headquarter_address = headquarter_address;
			this.head_city = head_city;
			this.head_state = head_state;
			this.campaign_id = campaign_id;
			this.api = api;
			this.region = region;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}
		
		public Boolean getUpdated() {
	        return updated;
	    }

	    public void setUpdated(Boolean updated) {
	        this.updated = updated;
	    }

		public String getPid() {
			return pid;
		}

		public void setPid(String pid) {
			this.pid = pid;
		}

		public String getFirst_name() {
			return first_name;
		}

		public void setFirst_name(String first_name) {
			this.first_name = first_name;
		}

		public String getLast_name() {
			return last_name;
		}

		public void setLast_name(String last_name) {
			this.last_name = last_name;
		}

		public String getEmail_address() {
			return email_address;
		}

		public void setEmail_address(String email_address) {
			this.email_address = email_address;
		}

		public String getCompany_name() {
			return company_name;
		}

		public void setCompany_name(String company_name) {
			this.company_name = company_name;
		}

		public String getCompany_domain() {
			return company_domain;
		}

		public void setCompany_domain(String company_domain) {
			this.company_domain = company_domain;
		}

		public String getJob_title() {
			return job_title;
		}

		public void setJob_title(String job_title) {
			this.job_title = job_title;
		}

		public String getJob_function() {
			return job_function;
		}

		public void setJob_function(String job_function) {
			this.job_function = job_function;
		}

		public String getJob_level() {
			return job_level;
		}

		public void setJob_level(String job_level) {
			this.job_level = job_level;
		}

		public String getCompany_address() {
			return company_address;
		}

		public void setCompany_address(String company_address) {
			this.company_address = company_address;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		public String getZip_code() {
			return zip_code;
		}

		public void setZip_code(String zip_code) {
			this.zip_code = zip_code;
		}

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public String getTelephone_number() {
			return telephone_number;
		}

		public void setTelephone_number(String telephone_number) {
			this.telephone_number = telephone_number;
		}

		public String getEmployee_size() {
			return employee_size;
		}

		public void setEmployee_size(String employee_size) {
			this.employee_size = employee_size;
		}

		public String getIndustry() {
			return industry;
		}

		public void setIndustry(String industry) {
			this.industry = industry;
		}

		public String getRevenue() {
			return revenue;
		}

		public void setRevenue(String revenue) {
			this.revenue = revenue;
		}

		public String getSic() {
			return sic;
		}

		public void setSic(String sic) {
			this.sic = sic;
		}

		public String getNaic() {
			return naic;
		}

		public void setNaic(String naic) {
			this.naic = naic;
		}

		public String getCompany_link() {
			return company_link;
		}

		public void setCompany_link(String company_link) {
			this.company_link = company_link;
		}

		public String getProspect_link() {
			return prospect_link;
		}

		public void setProspect_link(String prospect_link) {
			this.prospect_link = prospect_link;
		}

		public String getEmail_validation() {
			return email_validation;
		}

		public void setEmail_validation(String email_validation) {
			this.email_validation = email_validation;
		}

		public String getHeadquarter_address() {
			return headquarter_address;
		}

		public void setHeadquarter_address(String headquarter_address) {
			this.headquarter_address = headquarter_address;
		}

		public String getHead_city() {
			return head_city;
		}

		public void setHead_city(String head_city) {
			this.head_city = head_city;
		}

		public String getHead_state() {
			return head_state;
		}

		public void setHead_state(String head_state) {
			this.head_state = head_state;
		}

		public String getCampaign_id() {
			return campaign_id;
		}

		public void setCampaign_id(String campaign_id) {
			this.campaign_id = campaign_id;
		}

		public String getApi() {
			return api;
		}

		public void setApi(String api) {
			this.api = api;
		}

		public String getRegion() {
			return region;
		}

		public void setRegion(String region) {
			this.region = region;
		}
	
}
