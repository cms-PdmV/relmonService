/*
 * angular js controllers for relmon request service frontend.
 */
var SERVICE_ADDRESS = "http://188.184.185.27:80"

var relmon_request_service_frontend = angular.module(
    "relmon_request_service_frontend",
    ["ui.bootstrap"]
);

relmon_request_service_frontend.controller(
    "Request_controller",
    ["$http", "$modal", Request_controller]
);

function Request_controller_exception(message) {
    this.message = message;
}

function Request_controller($http, $modal) {

// public properties
    this.sample_inputs = [];
    this.new_request_name = "";
    this.new_request_threshold = 100;
    this.relmon_requests = [];
    this.posting_terminator = {};

// private variables
    var http_requests_in_progress = 0;
    var me = this;
    var report_categories = [
        "Data", "FullSim", "FastSim", "Generator",
        "FullSim_PU", "FastSim_PU"];

// private functions

    /*
     * 
     */
    function reset_sample_inputs() {
        me.sample_inputs = [];
        for (var x in report_categories) {
            var input = {
                "name": report_categories[x],
                "lists": {
                    "reference": {
                        "name": report_categories[x] + " reference samples",
                        "data": ""
                    },
                    "target": {
                        "name": report_categories[x] + " target samples",
                        "data": ""
                    }
                },
                "HLT": "both"
            };
            me.sample_inputs.push(input);
        }
        me.new_request_name = "";
        me.new_request_threshold = 100;
    }

    /*
     * 
     */
    function prepare_new_request_post_data() {
        var all_sample_lists_empty = true;
        // var exist_nonmatching_lists --
        // becomes true if in some category there is a non empty target list and
        // an empty reference list or vice versa
        var exist_nonmatching_lists = false;
        var post_data = {};
        post_data["name"] = me.new_request_name;
        post_data["threshold"] = me.new_request_threshold;
        post_data["categories"] = [];
        for (var i = 0; i < me.sample_inputs.length; i++){
            var category = me.sample_inputs[i];
            // ↓ clone category from input to category_for_post
            var category_for_post = JSON.parse(JSON.stringify(category));
            // ↓ split by whitespace and remove empty strigs
            var ref_samples = category.lists.reference
                .data.split(/\s+/g).filter(function(v){return v!==""});
            var targ_samples = category.lists.target
                .data.split(/\s+/g).filter(function(v){return v!==""});
            if (ref_samples.length > 0 || targ_samples.length > 0){
                all_sample_lists_empty = false;
                if (ref_samples.length == 0 || targ_samples.length == 0){
                    throw new Request_controller_exception(
                        "Exist nonmatching lists."
                    );
                }
            }
            category_for_post.lists = {};
            category_for_post.lists["reference"] = ref_samples;
            category_for_post.lists["target"] = targ_samples;
            post_data["categories"].push(category_for_post);
        }
        if (all_sample_lists_empty) {
            throw new Request_controller_exception(
                "All sample lists are empty."
            );
        }
        return post_data;
    }

    /*
     * 
     */
    function http_request_prepare() {
        http_requests_in_progress++;
    }
    var http_get_success = function(data, status){
        me.relmon_requests = data;
        console.log("Successful GET.");
    };
    var http_get_error = function(data, status){
        me.relmon_requests = "";
        me.open_error_modal("Server responded with status code: " + status + "\n" + data);
    };
    var http_post_success = function(data, status){
        console.log("Successful POST.");
        me.get_requests();
    };
    var http_post_error = function(data, status){
        me.open_error_modal("Server responded with status code: " + status + "\n" + data);
    };
    var http_finally = function(){
        if (http_requests_in_progress <= 0)
            throw { name: "Unexpected.",
                    message: "http_requests_in_progress < 0"}
        me.http_requests_in_progress--;
    };

    function submit_request(post_data) {
        http_request_prepare();
        $http.post(SERVICE_ADDRESS + "/requests",
                   post_data)
            .success(http_post_success)
            .error(http_post_error)
            .finally(http_finally);
        reset_sample_inputs();
    }
    // public methods

    this.sample_count_by_status = function(samples, status) {
        count = 0
        for(i = 0; i < samples.length; i++){
            if (samples[i].status == status){
                count++;
            }
        }
        return count;
    };

    this.get_badge_class_by_status = function(status) {
        switch (status) {
        case "NoDQMIO":
            return "badge-warning";
            // breaks not reachable
        case "NoROOT":
            return "badge-warning";
        case "DQMIO":
            return "badge-info";
        case "failed":
            return "badge-danger";
        case "downloaded":
            return "badge-success";
        case "ROOT":
            return "badge-info";
        default:
            return "";
        }
    };

    this.get_requests = function() {
        http_request_prepare();
        $http.get("requests")
            .success(http_get_success)
            .error(http_get_error)
            .finally(http_finally);
    };

    this.try_submit = function() {
        try {
            post_data = prepare_new_request_post_data();
            // TODO: format post_data text
            var modal_inst = this.open_confirm_modal(post_data);
            modal_inst.result.then(
                function() {
                    return submit_request(post_data);});
        } catch (e) {
            if (e instanceof Request_controller_exception) {
                this.open_error_modal(e.message);
            } else {
                throw e;
            }
        }
    };

    this.post_terminate = function(relmon_request) {
        var modal_inst = this.open_confirm_modal(
            "Campaign " + relmon_request["name"] +
            " is going to be terminated.\nDo you want to proceed?")
        modal_inst.result.then(
            function() {
                http_request_prepare();
                me.posting_terminator[relmon_request["id"]] = true;
                $http.post(SERVICE_ADDRESS + "/requests/" + relmon_request["id"] + "/terminate")
                    .success(http_post_success)
                    .error(http_post_error)
                    .finally(function(){
                        me.posting_terminator[relmon_request["id"]] = false;
                        me.http_finally();
                    });
            }
        );
    }

    this.open_confirm_modal = function(message) {
        var modal_inst = $modal.open( {
            templateUrl: "modals/Confirm_modal.htm",
            controller: "Confirm_modal_controller",
            resolve: {
                message: function () {
                    return message;
                }
            }
        });
        return modal_inst;
    };

    this.open_error_modal = function(message) {
        var modal_inst = $modal.open( {
            templateUrl: "modals/Error_modal.htm",
            controller: "Error_modal_controller",
            resolve: {
                message: function () {
                    return message;
                }
            }
        });
        return modal_inst;
    };

// init stuff
    reset_sample_inputs();
    this.get_requests();

}

relmon_request_service_frontend.controller(
    "Confirm_modal_controller", function($scope, $modalInstance, message) {
        $scope.message = message;
        $scope.confirm = function () {
            $modalInstance.close();
        };
        $scope.cancel = function () {
            $modalInstance.dismiss();
        };

});

relmon_request_service_frontend.controller(
    "Error_modal_controller", function($scope, $modalInstance, message) {
        $scope.message = message;
        $scope.cancel = function () {
            $modalInstance.dismiss();
        };

});
