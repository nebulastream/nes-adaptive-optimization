# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_test(distributed-planning-test DistributedPlanningTest.cpp)
target_link_libraries(distributed-planning-test nes-frontend-lib)
target_include_directories(distributed-planning-test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../private
)

add_nes_test(grpc-query-submission-backend-test GRPCQuerySubmissionBackendTest.cpp)
target_link_libraries(grpc-query-submission-backend-test nes-frontend-lib)

add_nes_test(drain-planner-test DrainPlannerTest.cpp)
target_link_libraries(drain-planner-test nes-frontend-lib)
target_include_directories(drain-planner-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_nes_test(query-manager-drain-test QueryManagerDrainTest.cpp)
target_link_libraries(query-manager-drain-test nes-frontend-lib)
target_include_directories(query-manager-drain-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_nes_test(drain-integration-test DrainIntegrationTest.cpp)
target_link_libraries(drain-integration-test nes-frontend-embedded-lib)
target_include_directories(drain-integration-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Starts nes-single-node-worker processes and drives them over gRPC.
add_nes_test(drain-grpc-test DrainGrpcTest.cpp)
target_link_libraries(drain-grpc-test nes-frontend-lib)
target_include_directories(drain-grpc-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(drain-grpc-test PRIVATE NES_WORKER_BINARY="$<TARGET_FILE:nes-single-node-worker>")
add_dependencies(drain-grpc-test nes-single-node-worker)
