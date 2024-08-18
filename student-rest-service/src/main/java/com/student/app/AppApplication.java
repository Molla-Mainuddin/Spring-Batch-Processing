package com.student.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
@RestController
@RequestMapping("api/v1")
public class AppApplication {

	public static void main(String[] args) {
		SpringApplication.run(AppApplication.class, args);
	}
	@GetMapping("/students")
	public List<StudentResponse> students(){
		return Arrays.asList(
				new StudentResponse(1L,"Molla","Mainuddin","mainuddin@gmail.com"),
				new StudentResponse(2L,"John","Doe","john@gmail.com"),
				new StudentResponse(3L,"Rima","Ray","rima@gmail.com"),
				new StudentResponse(4L,"Rahul","Kumar","rahul@gmail.com"),
				new StudentResponse(5L, "Murari", "Kumar","murari@gmail.com"),
				new StudentResponse(6l,"Sahan","Banarjee","sahana@gmail.com"),
				new StudentResponse(7L,"Bikram","Mondal","bikram@gmail.com"),
				new StudentResponse(8L,"Tanmoy","Pal","tanmoy@gmail.com"),
				new StudentResponse(9l,"Shanta", "Phani","shanta@gmail.com"),
				new StudentResponse(10,"Tania","Mondal","tania@gmail.com")
		);
	}
	@PostMapping("/createStudent")
	public StudentResponse createStudent(@RequestBody StudentRequest studentRequest){
		System.out.println("Student Created "+studentRequest.getId());
		return new StudentResponse(studentRequest.getId(),
			studentRequest.getFirstName(),
			studentRequest.getLastName(),
			studentRequest.getEmail()
			);
	}
}
