package app.vdb.service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import app.vdb.entity.Vector19;
import app.vdb.repository.Vector19Repository;

import java.util.List;

@Service
public class Vector19Service {

    @Autowired
    private Vector19Repository vector19Repository;

    @Autowired
    private ExclusionService exclusionService;


    public Page<Vector19> search(Specification<Vector19> spec, Pageable pageable) {
        return vector19Repository.findAll(spec, pageable);
    }


    public Page<Vector19> searchWithIncludedPids(Specification<Vector19> spec, Pageable pageable, String api) {
        List<String> includePids = exclusionService.getPidsFromTable(api);
        if (includePids.isEmpty()) {
            // Return an empty Page if there are no PIDs to include
            return Page.empty(pageable);
        }
        spec = spec.and((root, query, criteriaBuilder) -> root.get("pid").in(includePids));
        return vector19Repository.findAll(spec, pageable);
    }


    public long countWithExclusions(String api) {
        if (api != null && !api.isEmpty()) {
            List<String> excludePids = exclusionService.getPidsFromTable(api);
            if (!excludePids.isEmpty()) {
                return vector19Repository.count((root, query, criteriaBuilder) ->
                        criteriaBuilder.not(root.get("pid").in(excludePids))
                );
            }
        }
        return vector19Repository.count();
    }
    

}