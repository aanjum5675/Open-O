//CHECKSTYLE:OFF
/**
 * Copyright (c) 2024. Magenta Health. All Rights Reserved.
 * Copyright (c) 2008-2012 Indivica Inc.
 * <p>
 * This software is made available under the terms of the
 * GNU General Public License, Version 2, 1991 (GPLv2).
 * License details are available via "indivica.ca/gplv2"
 * and "gnu.org/licenses/gpl-2.0.html".
 * <p>
 * modifications made by Magenta Health in 2024.
 */
package org.oscarehr.common.dao;

import org.oscarehr.caseload.CaseloadCategory;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.math.BigInteger;
import java.util.*;

import org.springframework.transaction.annotation.Transactional;

@Transactional
public class CaseloadDaoImpl implements CaseloadDao {

    @PersistenceContext(unitName = "entityManagerFactory")
    protected EntityManager entityManager = null;

    // Assume query maps use ?1, ?2, etc.

    private String getSafeSearchQuery(String searchQuery) {
        return caseloadSearchQueries.get(searchQuery);
    }

    @SuppressWarnings("unchecked")
    public List<Integer> getCaseloadDemographicSet(String searchQuery, String[] searchParams, String[] sortParams, CaseloadCategory category, String sortDir, int page, int pageSize) {
        String demoQuery = getSafeSearchQuery(searchQuery);
        String query;

        if (category == CaseloadCategory.Demographic) {
            query = demoQuery + " ORDER BY last_name ?3, first_name ?4 LIMIT ?5, ?6";
        } else if (category == CaseloadCategory.Age) {
            int split = demoQuery.indexOf(",", demoQuery.indexOf("demographic_no"));
            query = demoQuery.substring(0, split) + ", CAST((DATE_FORMAT(NOW(), '%Y') - DATE_FORMAT(concat(year_of_birth,month_of_birth,date_of_birth), '%Y') - (DATE_FORMAT(NOW(), '00-%m-%d') < DATE_FORMAT(concat(year_of_birth,month_of_birth,date_of_birth), '00-%m-%d'))) as UNSIGNED INTEGER) as age " +
                    demoQuery.substring(split) + " ORDER BY ISNULL(age) ASC, age ?3, last_name ?4, first_name ?5 LIMIT ?6, ?7";
        } else if (category == CaseloadCategory.Sex) {
            int split = demoQuery.indexOf(",", demoQuery.indexOf("demographic_no"));
            query = demoQuery.substring(0, split) + ", sex " + demoQuery.substring(split) +
                    " ORDER BY sex = '' ASC, sex ?3, last_name ?4, first_name ?5 LIMIT ?6, ?7";
        } else {
            String sortQuery = caseloadSortQueries.get(category.getQuery());
            if (category.isMeasurement()) {
                query = "SELECT Y.demographic_no, Y.last_name, Y.first_name, X." + category.getField() +
                        " FROM (" + demoQuery + ") as Y LEFT JOIN (" + sortQuery + ") as X on Y.demographic_no = X.demographic_no ORDER BY ISNULL(X." + category.getField() + ") ASC, CAST(X." + category.getField() + " as DECIMAL(10,4)) ?3, Y.last_name ?4, Y.first_name ?5 LIMIT ?6, ?7";
            } else {
                query = "SELECT Y.demographic_no, Y.last_name, Y.first_name, X." + category.getField() +
                        " FROM (" + demoQuery + ") as Y LEFT JOIN (" + sortQuery + ") as X on Y.demographic_no = X.demographic_no ORDER BY ISNULL(X." + category.getField() + ") ASC, X." + category.getField() + " ?3, Y.last_name ?4, Y.first_name ?5 LIMIT ?6, ?7";
            }
        }

        Query q = entityManager.createNativeQuery(query);
        int paramIndex = 1;
        if (searchParams != null) {
            for (String param : searchParams) {
                try {
                    q.setParameter(paramIndex++, Integer.parseInt(param));
                } catch (NumberFormatException e) {
                    q.setParameter(paramIndex++, param);
                }
            }
        }

        q.setParameter(paramIndex++, sortDir);
        q.setParameter(paramIndex++, sortDir);
        q.setParameter(paramIndex++, sortDir);
        q.setParameter(paramIndex++, page * pageSize);
        q.setParameter(paramIndex++, pageSize);

        List<Object[]> result = q.getResultList();
        List<Integer> demographicNoList = new ArrayList<>();
        for (Object[] r : result) {
            demographicNoList.add((Integer) r[0]);
        }
        return demographicNoList;
    }

    public List<Map<String, Object>> getCaseloadDemographicData(String searchQuery, Object[] params) {
        String query = caseloadDemoQueries.get(searchQuery);
        String[] queryColumns = caseloadDemoQueryColumns.get(searchQuery);

        Query q = entityManager.createNativeQuery(query);
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                q.setParameter(i + 1, params[i]);
            }
        }

        if ("cl_measurement".equals(searchQuery)) {
            q.setMaxResults(1);
        }

        List<Object> result = q.getResultList();
        List<Map<String, Object>> dataResult = new ArrayList<>();

        for (Object r : result) {
            Map<String, Object> row = new HashMap<>();
            if (r instanceof Object[]) {
                for (int i = 0; i < ((Object[]) r).length; i++) {
                    row.put(queryColumns[i], ((Object[]) r)[i]);
                }
            } else {
                row.put(queryColumns[0], r);
            }
            dataResult.add(row);
        }
        return dataResult;
    }

    @SuppressWarnings("unchecked")
    public Integer getCaseloadDemographicSearchSize(String searchQuery, String[] searchParams) {
        String demoQuery = getSafeSearchQuery(searchQuery);
        String query = "SELECT count(1) AS count FROM (" + demoQuery + ") AS X";

        Query q = entityManager.createNativeQuery(query);
        int paramIndex = 1;
        if (searchParams != null) {
            for (String param : searchParams) {
                try {
                    q.setParameter(paramIndex++, Integer.parseInt(param));
                } catch (NumberFormatException e) {
                    q.setParameter(paramIndex++, param);
                }
            }
        }

        List<BigInteger> result = q.getResultList();
        return result.get(0).intValue();
    }
}