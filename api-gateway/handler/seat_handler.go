package handler

import (
	"encoding/json"
	"net/http"

	"api-gateway/pb"
)

type SeatHandler struct {
	seatClient pb.SeatServiceClient
}

func NewSeatHandler(seatClient pb.SeatServiceClient) *SeatHandler {
	return &SeatHandler{
		seatClient: seatClient,
	}
}

func (h *SeatHandler) GetSeats(w http.ResponseWriter, r *http.Request) {
	// 1. Ambil query parameter 'event_id'
	eventID := r.URL.Query().Get("event_id")
	if eventID == "" {
		http.Error(w, `{"error": "event_id query parameter is required"}`, http.StatusBadRequest)
		return
	}

	// 2. Panggil gRPC Seat Service secara synchronous
	grpcReq := &pb.GetSeatsRequest{
		EventId: eventID,
	}

	grpcResp, err := h.seatClient.GetSeats(r.Context(), grpcReq)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to fetch seats from Seat Service: " + err.Error(),
		})
		return
	}

	// 3. Kirim respon gRPC kembali sebagai HTTP JSON Response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(grpcResp)
}