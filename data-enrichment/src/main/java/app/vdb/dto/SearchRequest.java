package app.vdb.dto;


import java.util.HashMap;
import java.util.Map;

public class SearchRequest {

    private Map<String, String> filters = new HashMap<>();
    private String api;
    private int page = 1;
    private int size = 50;

    
    public Map<String, String> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, String> filters) {
        this.filters = filters;
    }

    public String getApi() {
        return api;
    }

    public void setApi(String api) {
        this.api = api;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}

