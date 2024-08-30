package app.vdb.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import app.vdb.dto.SearchRequest;
import app.vdb.entity.Vector19;
import app.vdb.service.ExclusionService;
import app.vdb.service.Vector19Service;
import jakarta.persistence.criteria.Predicate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@CrossOrigin(origins = {"http://localhost:4200", "http://192.168.0.5:4200", "https://test.vectordb.app/", "https://vectordb.app/"})
public class Vector19Controller {

    private static final Logger logger = LoggerFactory.getLogger(Vector19Controller.class);

    @Autowired
    private Vector19Service vector19Service;
    
    @Autowired
    private ExclusionService exclusionService;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @GetMapping("/v1/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "API is running");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/v1/search/net-new")
    public ResponseEntity<Map<String, Object>> search(@RequestBody SearchRequest request) {
        int zeroBasedPage = request.getPage() > 0 ? request.getPage() - 1 : 0;

        // Ensure filters are not null
        Map<String, String> filters = request.getFilters();
        if (filters == null) {
            filters = new HashMap<>();
        }

        Map<String, String> validParams = filterValidParams(filters);

        // Return only count if no filters have values
        if (validParams.isEmpty()) {
            long totalCount = vector19Service.countWithExclusions(request.getApi());
            Map<String, Object> response = new HashMap<>();
            response.put("net_new_count", totalCount);
            return ResponseEntity.ok(response);
        }

        Specification<Vector19> spec = buildSpecification(validParams, request.getApi(), true);
        Pageable pageable = PageRequest.of(zeroBasedPage, request.getSize());


        Page<Vector19> resultPage = vector19Service.search(spec, pageable);


        Map<String, Object> response = new HashMap<>();
        response.put("net_new_data", resultPage.getContent());
        response.put("net_new_pagination", generatePaginationInfo(resultPage));
        response.put("net_new_count", resultPage.getTotalElements());

        return ResponseEntity.ok(response);
    }

    @PostMapping("/v1/search/saved")
    public ResponseEntity<Map<String, Object>> searchApi(@RequestBody SearchRequest request) {
        int zeroBasedPage = request.getPage() > 0 ? request.getPage() - 1 : 0;

        Map<String, String> filters = request.getFilters();
        if (filters == null) {
            filters = new HashMap<>();
        }

        Map<String, String> validParams = filterValidParams(filters);

        List<Vector19> userProvidedData = fetchUserProvidedData(request.getApi());

        Specification<Vector19> spec = buildSpecification(validParams, null, false);
        Pageable pageable = PageRequest.of(zeroBasedPage, request.getSize());

        Page<Vector19> resultPage = vector19Service.searchWithIncludedPids(spec, pageable, request.getApi());

        List<Vector19> updatedResults = resultPage.getContent().stream().map(vector19 -> {
            boolean emailMatch = userProvidedData.stream().anyMatch(userProvided -> 
                vector19.getEmail_address() != null && vector19.getEmail_address().equals(userProvided.getEmail_address())
            );
            boolean companyNameMatch = userProvidedData.stream().anyMatch(userProvided -> 
                vector19.getCompany_name() != null && vector19.getCompany_name().equals(userProvided.getCompany_name())
            );
            boolean jobTitleMatch = userProvidedData.stream().anyMatch(userProvided -> 
                vector19.getJob_title() != null && vector19.getJob_title().equals(userProvided.getJob_title())
            );

            vector19.setemailStatus(emailMatch);
            vector19.setcompanyStatus(companyNameMatch);
            vector19.setjobTitleStatus(jobTitleMatch);

          

            return vector19;
        }).collect(Collectors.toList());

        Map<String, Object> response = new HashMap<>();
        response.put("saved_data", updatedResults);
        response.put("saved_pagination", generatePaginationInfo1(resultPage));
        response.put("saved_count", resultPage.getTotalElements());

        return ResponseEntity.ok(response);
    }


    private List<Vector19> fetchUserProvidedData(String api) {
        // Validate the table name to prevent SQL injection
        if (api == null || api.isEmpty() || !isValidTableName(api)) {
            throw new IllegalArgumentException("Invalid table name");
        }

        // Build the SQL query
        String sql = "SELECT * FROM " + api;

        try {
            // Fetch data from the user-provided table
            List<Vector19> userProvidedData = jdbcTemplate.query(sql, (rs, rowNum) -> {
                Vector19 vector19 = new Vector19();
                vector19.setId(rs.getLong("id")); // Ensure ID is set
                vector19.setEmail_address(rs.getString("email_address"));
                vector19.setCompany_name(rs.getString("company_name"));
                vector19.setJob_title(rs.getString("job_title"));
                vector19.setJob_function(rs.getString("job_function"));
                vector19.setJob_level(rs.getString("job_level"));
                vector19.setCompany_address(rs.getString("company_address"));
                vector19.setCity(rs.getString("city"));
                vector19.setState(rs.getString("state"));
                vector19.setZip_code(rs.getString("zip_code"));
                vector19.setCountry(rs.getString("country"));
                vector19.setTelephone_number(rs.getString("telephone_number"));
                vector19.setEmployee_size(rs.getString("employee_size"));
                vector19.setIndustry(rs.getString("industry"));
                vector19.setCompany_link(rs.getString("company_link"));
                vector19.setProspect_link(rs.getString("prospect_link"));
                vector19.setEmail_validation(rs.getString("email_validation"));
                vector19.setHeadquarter_address(rs.getString("headquarter_address"));
                vector19.setHead_city(rs.getString("head_city"));
                vector19.setHead_state(rs.getString("head_state"));
                vector19.setCampaign_id(rs.getString("campaign_id"));
                vector19.setApi(rs.getString("api"));
                vector19.setRegion(rs.getString("region"));
                return vector19;
            });

            // Convert list to a Set of IDs for fast lookup
            Set<String> userProvidedPids = userProvidedData.stream()
                .map(Vector19::getPid)
                .collect(Collectors.toSet());

            // Update the `saved` status in the list
            userProvidedData.forEach(vector19 -> vector19.setSaved(userProvidedPids.contains(vector19.getPid())));

            return userProvidedData;
        } catch (Exception e) {
            throw new RuntimeException("Error fetching data from table: " + api, e);
        }
    }


    private boolean isValidTableName(String tableName) {

        return tableName.matches("^[a-zA-Z0-9_]+$");
    }


    @PostMapping("/v1/search/total")
    public ResponseEntity<Map<String, Object>> searchVector19(@RequestBody SearchRequest request) {
        int zeroBasedPage = request.getPage() > 0 ? request.getPage() - 1 : 0;

        Map<String, String> filters = request.getFilters();
        if (filters == null) {
            filters = new HashMap<>();
        }

        Map<String, String> validParams = filterValidParams(filters);

        if (validParams.isEmpty()) {
            long totalCount = vector19Service.countWithExclusions(null);
            Map<String, Object> response = new HashMap<>();
            response.put("total_count", totalCount);
            return ResponseEntity.ok(response);
        }

        Specification<Vector19> spec = buildSpecification(validParams, null, false);
        Pageable pageable = PageRequest.of(zeroBasedPage, request.getSize());

        Page<Vector19> resultPage = vector19Service.search(spec, pageable);

        List<String> userProvidedPids = exclusionService.getPidsFromTable(request.getApi());

        List<Vector19> updatedResults = resultPage.getContent().stream().map(vector19 -> {
            boolean isSaved = userProvidedPids.contains(vector19.getPid());
            vector19.setSaved(isSaved);
            return vector19;
        }).collect(Collectors.toList());

        Map<String, Object> response = new HashMap<>();
        response.put("total_data", updatedResults);
        response.put("total_count", resultPage.getTotalElements());
        response.put("total_pages", generatePaginationInfo2(resultPage));

        return ResponseEntity.ok(response);
    }



    private static final int BATCH_SIZE = 1000;
    private List<List<String>> splitIntoBatches(List<String> values, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < values.size(); i += batchSize) {
            int end = Math.min(values.size(), i + batchSize);
            batches.add(values.subList(i, end));
        }
        return batches;
    }

    private Map<String, String> filterValidParams(Map<String, String> allParams) {
        Set<String> entityFields = Arrays.stream(Vector19.class.getDeclaredFields())
                .map(Field::getName)
                .collect(Collectors.toSet());

        Map<String, String> validParams = new HashMap<>();
        allParams.forEach((key, value) -> {
            if ((key.startsWith("include_") || key.startsWith("exclude_")) && !value.isEmpty() &&
                    entityFields.contains(key.replace("include_", "").replace("exclude_", ""))) {
                validParams.put(key, value.replace(", ", ","));
            }
        });
        return validParams;
    }

    private Specification<Vector19> buildSpecification(Map<String, String> validParams, String api, boolean useExclusion) {
        return (root, query, criteriaBuilder) -> {
            List<Predicate> predicates = new ArrayList<>();

            validParams.forEach((key, value) -> {
                if (key.startsWith("include_")) {
                    String field = key.replace("include_", "");
                    List<String> values = Arrays.asList(value.split(","));
                    predicates.add(root.get(field).in(values));
                } else if (key.startsWith("exclude_")) {
                    String field = key.replace("exclude_", "");
                    predicates.add(criteriaBuilder.not(root.get(field).in(Arrays.asList(value.split(",")))));
                }
            });

            if (useExclusion && api != null && !api.isEmpty()) {
                List<String> excludePids = exclusionService.getPidsFromTable(api);
                if (!excludePids.isEmpty()) {
                    predicates.add(criteriaBuilder.not(root.get("pid").in(excludePids)));
                }
            }

            return criteriaBuilder.and(predicates.toArray(new Predicate[0]));
        };
    }

    private Map<String, Object> generatePaginationInfo(Page<Vector19> resultPage) {
        Map<String, Object> paginationInfo = new HashMap<>();
        paginationInfo.put("current_page_net_new", resultPage.getNumber() + 1);
        paginationInfo.put("records_per_page", resultPage.getSize());
        paginationInfo.put("total_pages_net_new", resultPage.getTotalPages());
        return paginationInfo;
    }
    
    private Map<String, Object> generatePaginationInfo1(Page<Vector19> resultPage) {
        Map<String, Object> paginationInfo = new HashMap<>();
        paginationInfo.put("current_page_saved", resultPage.getNumber() + 1);
        paginationInfo.put("records_per_page", resultPage.getSize());
        paginationInfo.put("total_pages_saved", resultPage.getTotalPages());
        return paginationInfo;
    }
    
    private Map<String, Object> generatePaginationInfo2(Page<Vector19> resultPage) {
        Map<String, Object> paginationInfo = new HashMap<>();
        paginationInfo.put("current_page_total", resultPage.getNumber() + 1);
        paginationInfo.put("records_per_page_total", resultPage.getSize());
        paginationInfo.put("total_pages_total", resultPage.getTotalPages());
        return paginationInfo;
    }
}
