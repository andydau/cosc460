/* COSC 460 Fall 2014, Lab 3 */

/* These set the output format.  Please be sure to leave these settings as is. */
.header OFF
.mode list 

/* For each of the queries below, put your SQL in the place indicated by the comment.  
   Be sure to have all the requested columns in your answer, in the order they are 
   listed in the question - and be sure to sort things where the question requires 
   them to be sorted, and eliminate duplicates where the question requires that.   
   I will grade the assignment by running the queries on a test database and 
   eyeballing the SQL queries where necessary.  I won't grade on SQL style, but 
   we also won't give partial credit for any individual question - so you should be 
   confident that your query works. In particular, your output should match 
   the example output.
*/

/* Q1 -  Find the titles of all movies directed by Steven Spielberg.  */
select " ";
select "Q1";

/* Put your SQL for Q1 below */
select title from Movie where director='Steven Spielberg';

/* Q2 -  Find all years that have a movie that received a rating of 4 or 5, 
         and sort them in increasing order.             
*/
select " ";
select "Q2";

/* Put your SQL for Q2 below */
select distinct m.year from Movie m, Rating r where m.mID=r.mID and r.stars > 3 order by m.year;

/* Q3 -  Find the titles of all movies that have no ratings.
*/
select " ";
select "Q3";

/* Put your SQL for Q3 below */
select title from Movie where mID not in (select mID from Rating);

/* Q4 -  Write a query to return the ratings data in a more 
         readable format: reviewer name, movie title, stars, and ratingDate. 
         Also, sort the data, first by reviewer name, then by movie title, 
         and lastly by number of stars, all in ascending order.
*/
select " ";
select "Q4";

/* Put your SQL for Q4 below */
select re.name, m.title, ra.stars, ra.ratingDate from Rating ra, Reviewer re, Movie m where re.rID = ra.rID and ra.mID = m.mID order by re.name,m.title,ra.stars;

/* Q5 -  For all cases where the same reviewer rated the same movie twice 
         and gave it a higher rating the second time, return the reviewer's 
         name and the title of the movie.
*/
select " ";
select "Q5";

/* Put your SQL for Q5 below */
select re.name, m.title from Reviewer re, Movie m, 
	(select * from Rating r1, Rating r2 
	where r1.rID = r2.rID and r1.mID = r2.mID and r1.stars>r2.stars and r1.ratingDate>r2.ratingDate) ra
where ra.rID = re.rID and ra.mID = m.mID;

/* Q6 - For each movie that has at least one rating, find the highest number 
        of stars that movie received. Return the movie title and number of 
        stars. Sort by movie title. 
*/
select " ";
select "Q6";

/* Put your SQL for Q6 below */
select m.title, max(r.stars) from Movie m, Rating r where m.mID = r.mID group by r.mID order by m.title;

/* Q7 - For each movie, the title along with the number of ratings it has 
        received.  Your result should include those movies that have zero ratings.                                                                 
*/
select " ";
select "Q7";

/* Put your SQL for Q7 below */
select m.title,count(*) from Movie m, Rating r 
where m.mID = r.mID group by m.mID 
union 
select m.title,0 from Movie m 
where m.mID not in 
	(select distinct mID from Rating);


/* Q8 - For each movie that has at least one rating, return the title and the 
        'rating spread', that is, the difference between highest and lowest 
        ratings given to that movie. Sort by rating spread from highest to 
        lowest, then by movie title alphabetically.   
*/
select " ";
select "Q8";

/* Put your SQL for Q8 below */
select m.title, spread from Movie m, 
	(select mID, max(stars)-min(stars) as spread from Rating group by mID) r 
where m.mID = r.mID 
order by spread desc,m.title;


/* Q9 -  Find the difference between the average rating of movies released before 
         1980 and the average rating of movies released after 1980. (Make sure to 
         calculate the average rating for each movie, then the average of those 
         averages for movies before 1980 and movies after. Don't just calculate 
         the overall average rating before and after 1980.)  
*/
select " ";
select "Q9";

/* Put your SQL for Q9 below */
select avg(rating1)-avg(rating2) from 
	(select avg(r.stars) as rating1 from Movie m, Rating r 
	where m.mID = r.mID and m.year<1980 group by r.mID) r1, 
	(select avg(r.stars) as rating2 from Movie m, Rating r 
	where m.mID = r.mID and m.year>=1980 group by r.mID) r2;

/* Q10 - For each director, return the director's name together with the title(s) 
         of the movie(s) they directed that received the highest rating among all 
         of their movies, and the value of that rating. 
*/
select " ";
select "Q10";

/* Put your SQL for Q10 below */
select distinct t1.director, t1.title, t1.stars from 
	(select m.title,m.director,r.stars from Movie m, Rating r 
	where m.mID = r.mID) t1, 
	(select m.director,max(r.stars) as rate from Movie m, Rating r 
	where m.mID = r.mID group by m.director) t2 
where t1.director=t2.director and t1.stars=t2.rate;











