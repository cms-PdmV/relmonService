/*
 * angular js controllers for relmon request service frontend.
 */

var relmon_request_service_frontend = angular.module(
    "relmon_request_service_frontend",
    ["ui.bootstrap"]
);

relmon_request_service_frontend.controller(
    "Request_controller",
    ["$http", "$modal", "$location", Request_controller]
);

function Request_controller_exception(message) {
    this.message = message;
}

function Request_controller($http, $modal, $location) {

// public properties
    this.sample_inputs = [];
    this.new_request_name = "";
    this.new_request_threshold = 100;
    this.relmon_requests = [];
    this.user = {};
    this.new_request_collapsed = true;
    this.edit_button_show = false;
    this.internal_id;
    this.action_name = "Submit";
    this.old_name = "";
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
                category_for_post.lists = {};
                category_for_post.lists["reference"] = ref_samples;
                category_for_post.lists["target"] = targ_samples;
                post_data["categories"].push(category_for_post);
            }
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

    function submit_request() {
        http_request_prepare()
        if (typeof arguments[1] !== 'undefined') {
            $http.post("request/edit/" + arguments[1],
                    arguments[0])
            .success(http_post_success)
            .error(http_post_error)
            .finally(http_finally);
        } else {
            $http.post("requests",
                    arguments[0])
            .success(http_post_success)
            .error(http_post_error)
            .finally(http_finally);
        }
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
        case "failed_rqmgr":
            return "badge-warning";
        default:
            return "";
        }
    };
    
    this.get_requests = function() {
        http_request_prepare();
        $http.get("requests")
            .success(function(data, status){
                me.relmon_requests = data;
            })
            .error(function(data, status){
                me.relmon_requests = "";
                me.open_error_modal("Getting requests.\n" +
                                    "Server responded with status code: " +
                                    status + "\n" + data);
            })
            .finally(http_finally);
    };

    this.get_user_info = function() {
        http_request_prepare();
        $http.get("userinfo")
            .success(function(data, status){
                me.user = data;
            })
            .error(function(data, status){
                me.user.name = "unidentified";
                me.open_error_modal("Getting user information.\n" +
                                    "Server responded with status code: "
                                    + status + "\n" + data);
            })
            .finally(http_finally);
    };
    
    this.try_submit = function(id) {
        try {
            post_data = prepare_new_request_post_data();
            // TODO: format post_data text
            var modal_inst = this.open_confirm_modal(post_data);
            modal_inst.result.then(
                function() {
                    return submit_request(post_data, id);});
        } catch (e) {
            if (e instanceof Request_controller_exception) {
                this.open_error_modal(e.message);
            } else {
                throw e;
            }
        }
    };

    this.post_terminator = function(relmon_request) {
        var modal_inst = this.open_confirm_modal(
            "Campaign '" + relmon_request["name"] + "' is going to be terminated. " +
            "Existing comparison will be deleted. \nDo you want to proceed?")
        modal_inst.result.then(
            function() {
                http_request_prepare();
                $http.post("requests/" + relmon_request["id_"] + "/terminator")
                    .success(http_post_success)
                    .error(http_post_error)
                    .finally(function(){
                        me.http_finally();
                    });
            }
        );
    }

    this.post_closer = function(relmon_request) {
        var modal_inst = this.open_confirm_modal(
            "'" + relmon_request["name"] + "' records are going to be removed " +
            "from RelMon service. \nDo you want to proceed?")
        modal_inst.result.then(
            function() {
                http_request_prepare();
                $http.post("requests/" + relmon_request["id_"] + "/close")
                    .success(http_post_success)
                    .error(http_post_error)
                    .finally(function(){
                        me.http_finally();
                    });
            }
        );
    }

    this.post_edit = function(relmon_request, index) {
        reset_sample_inputs();
        $http.get("requests/" + relmon_request).success(function(data, status){
            _.each(data.categories, function(val, categIndex){
                correctIndex = report_categories.indexOf(val.name);
                _.each(data.categories[categIndex].lists, function(val, listIndex){
                    _.each(data.categories[categIndex].lists[listIndex], function(val){
                        me.sample_inputs[correctIndex].lists[listIndex].data += val.name + "\n";
                    })
                })
            })

            me.old_name = data.name;
            me.new_request_name = data.name;
            me.new_request_collapsed = false;
        });

        me.internal_id = relmon_request;
    }

    this.checkEditOrSubmit = function(){
        if ((me.new_request_name + "") == me.old_name) {
            me.action_name = "Edit"
        } else {
            me.action_name = "Submit"
        }
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
    this.get_user_info()
    this.get_requests();
}

// modal controllers

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
