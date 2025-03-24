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

    // Initialization blocks omitted for brevity
    // Assume caseloadSearchQueries, caseloadSortQueries, caseloadDemoQueries, caseloadDemoQueryColumns
    // are all initialized exactly as in your provided version

    private String getFormatedSearchQuery(String searchQuery, String[] searchParams) {
        if ("search_notes".equals(searchQuery)) {
            return String.format(caseloadSearchQueries.get(searchQuery), (Object[]) searchParams);
        } else {
            if (searchParams.length > 1) {
                Object[] tempParms = new Object[searchParams.length];
                System.arraycopy(searchParams, 0, tempParms, 0, searchParams.length - 1);
                tempParms[searchParams.length - 1] = Integer.parseInt(searchParams[searchParams.length - 1]);
                return String.format(caseloadSearchQueries.get(searchQuery), tempParms);
            } else {
                return String.format(caseloadSearchQueries.get(searchQuery), Integer.parseInt(searchParams[0]));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public List<Integer> getCaseloadDemographicSet(String searchQuery, String[] searchParams, String[] sortParams, CaseloadCategory category, String sortDir, int page, int pageSize) {
        String demoQuery = getFormatedSearchQuery(searchQuery, searchParams);
        String sortQuery;
        String query;

        if (category == CaseloadCategory.Demographic) {
            query = demoQuery + String.format(" ORDER BY last_name %s, first_name %s LIMIT %d, %d", sortDir, sortDir, page * pageSize, pageSize);
        } else if (category == CaseloadCategory.Age) {
            int split = demoQuery.indexOf(",", demoQuery.indexOf("demographic_no"));
            query = demoQuery.substring(0, split) + ", CAST((DATE_FORMAT(NOW(), '%Y') - DATE_FORMAT(concat(year_of_birth,month_of_birth,date_of_birth), '%Y') - (DATE_FORMAT(NOW(), '00-%m-%d') < DATE_FORMAT(concat(year_of_birth,month_of_birth,date_of_birth), '00-%m-%d'))) as UNSIGNED INTEGER) as age " + demoQuery.substring(split) + String.format(" ORDER BY ISNULL(age) ASC, age %s, last_name %s, first_name %s LIMIT %d, %d", sortDir, sortDir, sortDir, page * pageSize, pageSize);
        } else if (category == CaseloadCategory.Sex) {
            int split = demoQuery.indexOf(",", demoQuery.indexOf("demographic_no"));
            query = demoQuery.substring(0, split) + ", sex " + demoQuery.substring(split) + String.format(" ORDER BY sex = '' ASC, sex %s, last_name %s, first_name %s LIMIT %d, %d", sortDir, sortDir, sortDir, page * pageSize, pageSize);
        } else {
            sortQuery = sortParams != null ? String.format(caseloadSortQueries.get(category.getQuery()), (Object[]) sortParams) : caseloadSortQueries.get(category.getQuery());
            if (category.isMeasurement()) {
                query = String.format("SELECT Y.demographic_no, Y.last_name, Y.first_name, X.%s FROM (%s) as Y LEFT JOIN (%s) as X on Y.demographic_no = X.demographic_no ORDER BY ISNULL(X.%s) ASC, CAST(X.%s as DECIMAL(10,4)) %s, Y.last_name %s, Y.first_name %s LIMIT %d, %d",
                        category.getField(), demoQuery, sortQuery, category.getField(), category.getField(), sortDir, sortDir, sortDir, page * pageSize, pageSize);
            } else {
                query = String.format("SELECT Y.demographic_no, Y.last_name, Y.first_name, X.%s FROM (%s) as Y LEFT JOIN (%s) as X on Y.demographic_no = X.demographic_no ORDER BY ISNULL(X.%s) ASC, X.%s %s, Y.last_name %s, Y.first_name %s LIMIT %d, %d",
                        category.getField(), demoQuery, sortQuery, category.getField(), category.getField(), sortDir, sortDir, sortDir, page * pageSize, pageSize);
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
        if (sortParams != null) {
            for (String param : sortParams) {
                try {
                    q.setParameter(paramIndex++, Integer.parseInt(param));
                } catch (NumberFormatException e) {
                    q.setParameter(paramIndex++, param);
                }
            }
        }

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
        String demoQuery = getFormatedSearchQuery(searchQuery, searchParams);
        String query = String.format("SELECT count(1) AS count FROM (%s) AS X", demoQuery);

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

