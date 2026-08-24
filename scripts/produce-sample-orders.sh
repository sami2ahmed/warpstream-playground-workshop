#!/bin/bash

# Produce sample e-commerce orders to Kafka for Tableflow Lab 6
# Uses simple 6-field flat schema (no nested structures)

TOPIC=${1:-ecommerce-orders}
COUNT=${2:-10}
BROKER=${3:-localhost:9092}

echo "Producing $COUNT sample orders to topic: $TOPIC"
echo "Schema: order_id, customer_id, timestamp, total_amount, status, payment_method"
echo ""

for i in $(seq 1 $COUNT); do
  # Generate order data
  ORDER_ID=$(printf "ORD-%05d" $i)
  CUSTOMER_ID=$(printf "CUST-%05d" $(($RANDOM % 1000)))
  TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Random amount between 19.99 and 999.99
  AMOUNT=$(awk -v min=19.99 -v max=999.99 'BEGIN{srand(); printf "%.2f", min+rand()*(max-min)}')

  # Randomize status
  STATUSES=("pending" "confirmed" "shipped" "delivered" "cancelled")
  STATUS=${STATUSES[$RANDOM % ${#STATUSES[@]}]}

  # Randomize payment method
  PAYMENTS=("credit_card" "debit_card" "paypal" "apple_pay" "google_pay")
  PAYMENT=${PAYMENTS[$RANDOM % ${#PAYMENTS[@]}]}

  # Build simple JSON (6 fields only)
  JSON="{\"order_id\":\"$ORDER_ID\",\"customer_id\":\"$CUSTOMER_ID\",\"timestamp\":\"$TIMESTAMP\",\"total_amount\":$AMOUNT,\"status\":\"$STATUS\",\"payment_method\":\"$PAYMENT\"}"

  # Produce to Kafka
  echo "$JSON" | kcat -b $BROKER -t $TOPIC -P

  # Show progress every 10 orders
  if [ $(($i % 10)) -eq 0 ]; then
    echo "Produced $i/$COUNT orders"
  fi
done

echo ""
echo "Done! Produced $COUNT orders to $TOPIC"
echo ""
echo "Verify with:"
echo "  kcat -b $BROKER -t $TOPIC -C -o beginning -c 5 | jq"
