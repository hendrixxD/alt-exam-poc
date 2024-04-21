
-- PART 2a
----------------------------------------------------------------

-- Some Obeservations on the events table
--   1. items that only got visited and never got added to the cart or quantity specified
--   2. quantity to some items where specified, those that got added to the cart and those who were removed.
--   3. some items quantity never got specified, but were added to the cart, others were removed.
--   4. other items were checked out immediately. some which were successfull and others not.
--   5. items whose quantity never got specified were abandoned most probably



-- Q1.

-- I employed top-down approach, considering the ERD diagram,
-- i.e starting from the events table, then the customers table, then the orders table, then the line_items table then lastly the products table.

-- here from the events table, distinct customer_id's whose event_type was 'add_to_cart' are filtered
WITH items_added_to_cart AS (
    SELECT DISTINCT
        customer_id
    FROM
        alt_school.events
    WHERE
        event_data ->> 'event_type' = 'add_to_cart'
),
-- here, in the orders table, the `status` columns tells wether or not an item checked out succesfully, or was cancelled, or the order failed.
most_ordered_item AS (
    SELECT
        pr.id,
        pr.name,
        -- counts the number of successful checkouts for each item
        COUNT(o.order_id) AS number_of_sucfl_checkouts
    FROM
        alt_school.orders o
    JOIN
        alt_school.line_items li ON o.order_id = li.order_id 
    JOIN
        alt_school.products pr ON pr.id = li.item_id 
    WHERE
        -- here, orders to only customers who added items to their cart is filtered.
        o.customer_id IN (SELECT customer_id FROM items_added_to_cart)
        -- and orders who were successfull too as well
        AND o.status = 'success'
    GROUP BY
        pr.id, pr.name
    ORDER BY
        number_of_sucfl_checkouts DESC
    -- since we are interested in the most ordered item, the result is reduced to 1 row
    LIMIT 1
)
SELECT * FROM most_ordered_items;


-- Q2

-- finding the top 5 spenders -
--   entails that a customer has: 
--    1. added an item to a cart which its quantity was specified and checked out successfuly
--    or cheked out on an item which was not added to tha cart(such event types didnt tell us the quantity specified.

-- distinct customer_id are filtered from the alt_school.events table.
-- The WHERE clause uses two conditions with OR logic:
--   Customers who added items to their cart (event_type='add_to_cart' with a positive quantity)
--   Customers who completed a successful checkout (event_type='checkout' with status='success')

WITH customers_who_checkout_atleast_an_item AS (
    SELECT DISTINCT
        customer_id
    FROM
        alt_school.events
    WHERE
        (event_data ->> 'event_type' = 'add_to_cart' AND (event_data ->> 'quantity')::int > 0)
        OR
        (event_data ->> 'event_type' = 'checkout' AND event_data ->> 'status' = 'success')
),
-- here, the cte below groups the results by customer ID and location,
-- calculates the sum of the prices of all items purchased by each customer,
-- and sorts the customers based on their total amount spent in descending order
top_5_spenders AS (
    SELECT
        c.customer_id,
        c.location,
        SUM(pr.price) AS total_amount_spent
    FROM
        alt_school.customers c
    JOIN
        alt_school.orders o ON c.customer_id = o.customer_id
    JOIN
        alt_school.line_items li ON o.order_id = li.order_id 
    JOIN
        alt_school.products pr ON pr.id = li.item_id 
    JOIN
        -- customers who have checked out at least one item are filtered out
        customers_who_checkout_atleast_an_item cw ON cw.customer_id = o.customer_id
    WHERE
        -- the items to the respective customer IDs whose status ID is 'success' are filtered out
        o.status = 'success'
    GROUP BY
        c.customer_id, c.location
    ORDER BY
        total_amount_spent DESC
    LIMIT 5
)

SELECT * FROM top_5_spenders;



-- PART 2b
----------------------------------------------------------------

-- Q1

-- distinct customer_id are filtered from the alt_school.events table.
-- The WHERE clause uses two conditions with OR logic:
--   Customers who added items to their cart (event_type='add_to_cart' with a positive quantity)
--   Customers who completed a successful checkout (event_type='checkout' with status='success')

WITH valid_c_ids_who_checkout AS (
    SELECT DISTINCT
        customer_id
    FROM
        alt_school.events
    WHERE
        (event_data ->> 'event_type' = 'add_to_cart' AND (event_data ->> 'quantity')::int > 0)
        OR
        (event_data ->> 'event_type' = 'checkout' AND event_data ->> 'status' = 'success')
),
-- without applying rank, a group of most checked out locations have the same number of counts
-- and so, the most common location where successfull checkout occured, the result is queried futher to RANK by the number of successful checkout

-- successful orders are filtered (o.status = 'success').
-- the data is grouped by c.location (customer location) and counts the number of successful orders per location using COUNT(o.status) AS checkout_count.
-- most checkouts first, locations are rank by checkout count in descending order using the RANK() function with ORDER BY COUNT(o.status) DESC.
most_common__locations AS (
    SELECT
        c.location,
        COUNT(o.status) AS checkout_count,
        RANK() OVER (ORDER BY COUNT(o.status) DESC) AS rank
    FROM
        alt_school.customers c
    JOIN
        alt_school.orders o ON c.customer_id = o.customer_id
    JOIN
        alt_school.line_items li ON o.order_id = li.order_id 
    JOIN
        alt_school.products pr ON pr.id = li.item_id 
    JOIN
        valid_c_ids_who_checkout cw ON cw.customer_id = o.customer_id
    WHERE
        o.status = 'success'
    GROUP BY
        c.location
)
-- the result is then filtered(rank=1) again to get the top most common location successfull checkout occured and its count, 
-- location   checkout_count
-- Korea	  126
SELECT
    location,
    checkout_count
FROM
    most_common__locations
WHERE
    rank = 1;


-- Q2

-- Approach
-- 1.
-- abandoned carts are identified by looking for events where items are added to the cart (event_type is "add_to_cart")
-- but there is no corresponding successful checkout event (event_type is "checkout" and status is "success") for the same customer_id.
-- items whose quantity werent specified were mostly probably abandoned!!
-- 2.
--  havent identified abandoned carts, a count of the number of events
-- (excluding visits and checkout attempts) that occurred before the abandonment.
-- 3.
-- The event_timestamp field is used to determine the order of events.
-- 4.
-- I am relying on events table as the most single source of truth
-- and the checkout timestamp in the orders table is not reliable enough to depend on.
-- Instead, the timestamp of the last event before abandonment will be considered.

-- Identifies customers with abandoned carts
WITH abandoned_carts AS (
    SELECT
        customer_id
    FROM
        alt_school.events
    WHERE
        event_data ->> 'event_type' = 'add_to_cart'
        -- here a subquery is used to exclude customers who have a successful checkout for any event.
        -- This ensures only customers who added items but didn't successfully checkout are included.
        AND customer_id NOT IN (
            SELECT DISTINCT
         		customer_id
            FROM
            	alt_school.events
            WHERE
            	event_data ->> 'event_type' = 'checkout'
            AND
            	event_data ->> 'status' = 'success'
        )
),
-- Finds the last non-visit event timestamp for each customer with an abandoned cart.
last_event_before_abandonment AS (
    SELECT
        e.customer_id,
        -- pinpoints the most recent timestamp when the last event occurred before the cart was abandoned for each customer.
        MAX(event_timestamp) AS last_event_timestamp
    FROM
        alt_school.events e
    JOIN
        abandoned_carts ac ON e.customer_id = ac.customer_id
    where
        event_data ->> 'event_type' != 'visit'
    GROUP by
        -- GROUPs e.customer_id to find the latest non-visit event for each customer with an abandoned cart.
        e.customer_id
)
-- the number of non-visited events before abandonment for each customer is found.
SELECT
    e.customer_id,
    COUNT(*) AS num_events_before_abandonment
FROM
    alt_school.events e
JOIN
    last_event_before_abandonment le ON e.customer_id = le.customer_id
WHERE
	-- here, the events to be before or at the last non-visited event timestamp 
	-- to ensure events happened before the assumed abandonment time are filtered.
    e.customer_id IN (SELECT customer_id FROM abandoned_carts)
    AND
    	event_data ->> 'event_type' != 'visit'
    AND
    	event_timestamp <= le.last_event_timestamp
GROUP BY
    e.customer_id;


-- Q3
-- it is assumed that before a succesful checkout is made, a customer has to visit first.

-- customer_id's on the events table where event_type is 'visit' are filtered out, its visitd count is taken.
-- Then for successfull order, distinct customer_id's filtered above is
-- filtered again for only customer_id that appear on the orders table whose status was success.

SELECT
  ROUND(AVG(num_visits), 2) AS average_visits
FROM (
  SELECT
    e.customer_id,
    COUNT(*) AS num_visits
  FROM
    alt_school.events e
  WHERE
    event_data ->> 'event_type' = 'visit'
  GROUP BY
    e.customer_id
) AS customer_visits
WHERE customer_id IN (
  SELECT DISTINCT
    customer_id
  FROM
    alt_school.orders
  WHERE
    status = 'success'
);