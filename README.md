# amadeus-challenge-scala

## Introduction
This repository was created as part of the Scala challenge provided by Amadeus. It consist in four exercise described below, and was my first contact with scala.

## 
**Question 1: reading files**

Check the number of lines in each of the two files (bookings and searches)

**Question 2: top 10 arrival airports in the world in 2013 (using the bookings file)**

Arrival airport is the column arr_port. It is the IATA code for the airport

To get the total number of passengers for an airport, you can sum the column pax, grouping by arr_port. Note that there is negative pax. That corresponds to cancelations. So to get the total number of passengers that have actually booked, you should sum including the negatives (that will remove the canceled bookings).

Print the top 10 arrival airports in the standard output, including the number of passengers.
Required: Get the name of the city or airport corresponding to that airport (programmatically, we suggest to have a look at OpenTravelData in Github)

 
**Question 3: Matching searches with bookings**

For every search in the searches file, find out whether the search ended up in a booking or not (using the info in the bookings file). For instance, search and booking origin and destination should match. For the bookings file, origin and destination are the columns dep_port and arr_port, respectively. Generate a CSV file with the search data, and an additional field, containing 1 if the search ended up in a booking, and 0 otherwise.

**Bonus questions: write a Web Service**

Wrap the output of the second exercise in a web service that returns the data in JSON format (instead of printing to the standard output). The web service should accept a parameter n>0. For the top 10 airports, n is 10. For the X top airports, n is X.

## Solution
To conduct the challenge I used *Spark* for processing the data, *Scalatest* to create tests and *Scalatra* for the web service
