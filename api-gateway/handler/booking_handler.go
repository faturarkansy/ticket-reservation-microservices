package handler

import (
	"encoding/json"
	"net/http"

	bookingpb "api-gateway/pb/bookingpb"
)

type BookingHandler struct {
	bookingClient bookingpb.BookingServiceClient
}

func NewBookingHandler(bookingClient bookingpb.BookingServiceClient) *BookingHandler {
	return &BookingHandler{
		bookingClient: bookingClient,
	}
}

type CreateBookingRequestPayload struct {
	UserID  string `json:"user_id"`
	EventID string `json:"event_id"`
	SeatID  string `json:"seat_id"`
}

func (h *BookingHandler) CreateBooking(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var reqPayload CreateBookingRequestPayload
	if err := json.NewDecoder(r.Body).Decode(&reqPayload); err != nil {
		http.Error(w, `{"error": "Invalid request payload"}`, http.StatusBadRequest)
		return
	}

	grpcReq := &bookingpb.CreateBookingRequest{
		UserId:  reqPayload.UserID,
		EventId: reqPayload.EventID,
		SeatId:  reqPayload.SeatID,
	}

	grpcResp, err := h.bookingClient.CreateBooking(r.Context(), grpcReq)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to create booking: " + err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(grpcResp)
}