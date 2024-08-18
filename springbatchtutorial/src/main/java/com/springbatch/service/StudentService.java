package com.springbatch.service;

import com.springbatch.model.StudentCsv;
import com.springbatch.model.StudentResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

@Service
public class StudentService {
    List<StudentResponse> list = null;

    public List<StudentResponse> restCallToGetStudents() {
        RestTemplate restTemplate = new RestTemplate();
        StudentResponse[] studentResponseArray =
                restTemplate.getForObject("http://localhost:8081/api/v1/students", StudentResponse[].class);
        list = new ArrayList<>();
        for (StudentResponse sr : studentResponseArray) {
            list.add(sr);
        }
        return list;
    }

    public StudentResponse getStudent() {
        if (list == null) {
            restCallToGetStudents();
        }
        if (list != null && !list.isEmpty()) {
            return list.remove(0);
        }
        return null;
    }

    public StudentResponse restCallToCreateStudent(StudentCsv studentCsv) {
        RestTemplate restTemplate = new RestTemplate();
        return restTemplate.postForObject("http://localhost:8081/api/v1/createStudent",
                studentCsv,
                StudentResponse.class
        );
    }
}
