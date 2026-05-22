
var System = {

    'particle' : null,
    'particleGeo' : null,
    'particleColor' : [],
    'particleInfos' : [],
    'count' : 0,
    'scaleSize' : 64,
    'opacity' : 0.76,

    /**
     * Add a system in galaxy
     *
     * @param  {object} val        System properties (x, y, z, name are mandatory)
     * @param  {string} withSolid  Add a solid sphere (default: false)
     */

    'create' : function(val, withSolid) {

        if(withSolid==undefined) withSolid = false;

        if(val.coords==undefined) return false;

        var x = parseInt(val.coords.x);
        var y = parseInt(val.coords.y);
        var z = -parseInt(val.coords.z); //-- Revert Z coord
        var radius = Number(val.coords.radius); // sphere radius
        var sphereColor = val.coords?.sphereColor ?? 0x0E7F88; // sphere color
        var sphereOpacity = val.coords?.sphereOpacity ?? 0.5; // sphere opacity
        var sphereGeometryWidth = parseInt(val.coords?.sphereGeometryWidth ?? 64, 10);
        var sphereGeometryHeight = parseInt(val.coords?.sphereGeometryHeight ?? 32, 10);
        var hidePoint = val.hidePoint === true;
        if(!Number.isFinite(sphereGeometryWidth) || sphereGeometryWidth < 3) sphereGeometryWidth = 64;
        if(!Number.isFinite(sphereGeometryHeight) || sphereGeometryHeight < 2) sphereGeometryHeight = 32;

        //--------------------------------------------------------------------------
        //-- Particle for near and far view

        var colors = [];
        if(this.particleGeo !== null && !hidePoint) {

            //-- If system with info already registered, concat datas
            var idSys = x+'_'+y+'_'+z;
            if(val.infos != undefined && this.particleInfos[idSys] !== undefined) {
                var indexParticle = this.particleInfos[idSys];
                this.particleGeo.vertices[indexParticle].infos += val.infos;
                if(val.cat != undefined) Ed3d.addObjToCategories(indexParticle,val.cat);
                return;
            }

            var particle = new THREE.Vector3(x, y, z);

            //-- Get point color

            if(val.cat != undefined && val.cat[0] != undefined && Ed3d.colors[val.cat[0]] != undefined) {
                this.particleColor[this.count] = Ed3d.colors[val.cat[0]];
            } else {
                this.particleColor[this.count] = new THREE.Color(Ed3d.systemColor);
            }

            //-- If system got some categories, add it to cat list and save his main color

            if(val.cat != undefined) {
                Ed3d.addObjToCategories(this.count,val.cat);
                particle.color = this.particleColor[this.count];
            }

            //-- Attach name and set point as clickable

            particle.clickable = true;
            particle.visible = true;
            particle.name = val.name;
            if(val.infos != undefined) {
                particle.infos = val.infos;
                this.particleInfos[idSys] = this.count;
            }
            if(val.url != undefined) {
                particle.url = val.url;
            }

            this.particleGeo.vertices.push(particle);

            this.count++;
        }

        //--------------------------------------------------------------------------
        //-- Check if we have to add coords for a route

        if(Route.active == true) {

            if(Route.systems[val.name] != undefined) {
                Route.systems[val.name] = [x,y,z]
            }

        }

        //--------------------------------------------------------------------------
        //-- Build a sphere if needed
        if(withSolid && (!Number.isFinite(radius) || radius === 0)) {
            if(val.forceSolidAnchor) {
                var anchor = new THREE.Object3D();
                anchor.name = val.name;
                anchor.position.set(x, y, z);
                anchor.clickable = false;
                scene.add(anchor);
                return anchor;
            }
            return;
        }

        if(withSolid && radius != 0) {

            //-- Add glow sprite from first cat color if defined, else take white glow

            var mat = Ed3d.material.glow_1;

            var sprite = new THREE.Sprite( mat );
            sprite.position.set(x, y, z);
            sprite.scale.set(50, 50, 1.0);
            //scene.add(sprite); // this centers the glow at the mesh

            // Sagittarius A*
            var geometry, edges, spherePoints;

            // Create geometry based on radius
            if (radius > 0) {
                geometry = new THREE.SphereGeometry(radius, sphereGeometryWidth, sphereGeometryHeight);
            } else {
                geometry = new THREE.SphereGeometry(-radius, 8, 8);
                sphereColor = 0x000000;
                sphereOpacity = 0.01;
            }

            // Extract edges for dotted wireframe effect
            edges = new THREE.EdgesGeometry(geometry);

            // Create points material
            var pointsMaterial = new THREE.PointsMaterial({
                color: sphereColor || 0xffffff,
                size: 0.1, // size of each point
                transparent: true,
                opacity: sphereOpacity || 1
            });

            // Create points object using edges
            spherePoints = new THREE.Points(edges, pointsMaterial);

            // Set position
            spherePoints.position.set(x, y, z);

            // Assign metadata
            spherePoints.name = val.name;
            spherePoints.clickable = false;
            spherePoints.idsprite = sprite.id;

            // Add to scene
            scene.add(spherePoints);

            return spherePoints;
        }
    },


    /**
     * Init the galaxy particle geometry
     */

    'initParticleSystem' : function () {
        this.particleGeo = {
            vertices: [],
            colors: [],
            colorsNeedUpdate: false
        };
    },

    'syncParticleColors' : function () {

        if(this.particle == null || this.particle.geometry == null || this.particleGeo == null) return;

        var colorAttr = this.particle.geometry.getAttribute('color');
        if(colorAttr == null) return;

        for (var i = 0; i < this.particleGeo.colors.length; i++) {
            var color = this.particleGeo.colors[i] || new THREE.Color(Ed3d.systemColor);
            colorAttr.setXYZ(i, color.r, color.g, color.b);
        }

        colorAttr.needsUpdate = true;
        this.particleGeo.colorsNeedUpdate = false;
    },

    /**
     * Create the particle system
     */

    'endParticleSystem' : function () {

        if(this.particleGeo == null) return;

        this.particleGeo.colors = this.particleColor;

        var particleCount = this.particleGeo.vertices.length;
        var positions = new Float32Array( particleCount * 3 );
        var colors = new Float32Array( particleCount * 3 );

        for (var i = 0; i < particleCount; i++) {
            var particle = this.particleGeo.vertices[i];
            var color = this.particleGeo.colors[i] || new THREE.Color(Ed3d.systemColor);

            positions[ (i * 3) ] = particle.x;
            positions[ (i * 3) + 1 ] = particle.y;
            positions[ (i * 3) + 2 ] = particle.z;

            colors[ (i * 3) ] = color.r;
            colors[ (i * 3) + 1 ] = color.g;
            colors[ (i * 3) + 2 ] = color.b;
        }

        var particleGeometry = new THREE.BufferGeometry();
        particleGeometry.setAttribute( 'position', new THREE.BufferAttribute( positions, 3 ) );
        particleGeometry.setAttribute( 'color', new THREE.BufferAttribute( colors, 3 ) );

        var particleMaterial = new THREE.PointsMaterial({
            alphaMap: Ed3d.textures.flare_yellow,
            vertexColors: true,
            color: 0xffffff,
            size: this.scaleSize,
            fog: false,
            blending: THREE.AdditiveBlending,
            transparent: true,
            opacity: this.opacity,
            depthTest: true,
            depthWrite: false
        });

        this.particle = new THREE.Points(particleGeometry, particleMaterial);
        this.particle.clickable = true;

        scene.add(this.particle);
    },


    /**
     * Remove systems list
     */

    'remove' : function() {

        this.particleColor = [];
        this.particleGeo = null;
        this.particleInfos = [];
        this.count = 0;
        if(this.particle && this.particle.geometry) this.particle.geometry.dispose();
        if(this.particle && this.particle.material) this.particle.material.dispose();
        scene.remove(this.particle);
        this.particle = null;

    },

    /**
     * Load Spectral system color
     */

    'loadSpectral' : function(val) {

    }

}
